
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	op     string
	status int
	lat    time.Duration
	err    string
}

func main() {
	base := flag.String("base", env("LOADTEST_BASE", "https:"), "API origin")
	n := flag.Int("n", 2000, "number of users to create")
	c := flag.Int("c", 200, "requests in flight at once")
	requests := flag.Int("requests", 100000, "total HTTP requests to spread across -duration")
	duration := flag.Duration("duration", 15*time.Minute, "window the requests are spread across")
	prefix := flag.String("prefix", "", "username prefix (default lt<unix>)")
	password := flag.String("password", "LoadTest12", "password for every created user (8+ chars, letter and digit)")
	flag.Parse()

	if *n < 1 || *c < 1 {
		fmt.Fprintln(os.Stderr, "-n and -c must be >= 1")
		os.Exit(2)
	}
	if *requests < 1 || *duration <= 0 {
		fmt.Fprintln(os.Stderr, "-requests and -duration must be > 0")
		os.Exit(2)
	}
	gap := *duration / time.Duration(*requests)
	tag := *prefix
	if tag == "" {
		tag = fmt.Sprintf("lt%d", time.Now().Unix())
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *c,
			MaxIdleConnsPerHost: *c,
		},
	}
	ctx := context.Background()

	fmt.Printf("base=%s users=%d concurrency=%d requests=%d duration=%s gap=%s prefix=%s\n", *base, *n, *c, *requests, *duration, gap, tag)
	fmt.Printf("each request is one signup, login, or page, mixed and spread over %s\n", *duration)
	if err := checkSignupOpen(ctx, client, *base); err != nil {
		fmt.Printf("signup-open check skipped: %v\n", err)
	}

	slots := make(chan struct{})
	var wg sync.WaitGroup
	var results []result
	var mu sync.Mutex
	type account struct {
		name    string
		access  string
		refresh string
	}
	var accounts []account
	var next atomic.Int64
	var sent atomic.Int64

	record := func(user string, r result) {
		mu.Lock()
		results = append(results, r)
		line := fmt.Sprintf("%s %-32s status=%d latency=%s", time.Now().Format("15:04:05"), r.op, r.status, r.lat.Round(time.Millisecond))
		if user != "" {
			line += " user=" + user
		}
		if r.err != "" {
			line += " error=" + r.err
		}
		fmt.Println(line)
		mu.Unlock()
	}

	for i := 0; i < *c; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range slots {
				sent.Add(1)
				mu.Lock()
				have := len(accounts)
				var acct account
				if have > 0 {
					acct = accounts[rand.IntN(have)]
				}
				issued := int(next.Load())
				mu.Unlock()

				switch {
				case have == 0 || (issued < *n && rand.IntN(25) == 0):
					id := int(next.Add(1) - 1)
					if id >= *n {
						continue
					}
					user := fmt.Sprintf("%s_%d", tag, id)
					t0 := time.Now()
					status, body, err := postJSON(ctx, client, *base+"/v1/auth/signup", map[string]string{
						"username": user,
						"password": *password,
						"exam":     "JEE",
					})
					if err != nil || (status != http.StatusOK && status != http.StatusCreated) {
						record(user, result{"signup", status, time.Since(t0), errString(err, body)})
						continue
					}
					record(user, result{"signup", status, time.Since(t0), ""})
					access, refresh := tokensFrom(body)
					mu.Lock()
					accounts = append(accounts, account{user, access, refresh})
					mu.Unlock()

				case acct.access == "" || rand.IntN(20) == 0:
					t1 := time.Now()
					status, body, err := postJSON(ctx, client, *base+"/v1/auth/login", map[string]string{
						"username": acct.name,
						"password": *password,
					})
					if err != nil || status != http.StatusOK {
						record(acct.name, result{"login", status, time.Since(t1), errString(err, body)})
						continue
					}
					record(acct.name, result{"login", status, time.Since(t1), ""})
					access, refresh := tokensFrom(body)
					mu.Lock()
					for i := range accounts {
						if accounts[i].name == acct.name {
							accounts[i].access = access
							accounts[i].refresh = refresh
							break
						}
					}
					mu.Unlock()

				case acct.refresh != "" && rand.IntN(30) == 0:
					t1 := time.Now()
					status, body, err := postJSON(ctx, client, *base+"/v1/auth/refresh", map[string]string{
						"refresh_token": acct.refresh,
					})
					msg := ""
					if err != nil || status != http.StatusOK {
						msg = errString(err, body)
					}
					record(acct.name, result{"/v1/auth/refresh", status, time.Since(t1), msg})
					if msg == "" {
						access, refresh := tokensFrom(body)
						mu.Lock()
						for i := range accounts {
							if accounts[i].name == acct.name {
								if access != "" {
									accounts[i].access = access
								}
								if refresh != "" {
									accounts[i].refresh = refresh
								}
								break
							}
						}
						mu.Unlock()
					}

				default:
					path := crawlPaths[rand.IntN(len(crawlPaths))]
					t1 := time.Now()
					status, body, err := getAuth(ctx, client, *base+path, acct.access)
					msg := ""
					if err != nil || status < 200 || status >= 300 {
						msg = errString(err, body)
					}
					record(acct.name, result{path, status, time.Since(t1), msg})
				}
			}
		}()
	}

	start := time.Now()
	for i := 0; i < *requests; i++ {
		if i > 0 {
			time.Sleep(time.Duration(float64(gap) * (0.5 + rand.Float64())))
		}
		slots <- struct{}{}
	}
	close(slots)
	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("\nfinished in %s — requests sent: %d\n", elapsed.Round(time.Millisecond), sent.Load())
	printStats(results)
	fmt.Printf("\nusers look like %s_0 … %s_%d (password set via -password)\n", tag, tag, *n-1)
}

func checkSignupOpen(ctx context.Context, client *http.Client, base string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/v1/auth/signup-open", nil)
	if err != nil {
		return err
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("signup-open %d %s", res.StatusCode, truncate(body))
	}
	var v struct {
		SignupEnabled bool `json:"signup_enabled"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return err
	}
	if !v.SignupEnabled {
		return fmt.Errorf("signup is disabled on this API")
	}
	return nil
}

var crawlPaths = []string{
	"/v1/me",
	"/v1/syllabus",
	"/v1/inventory",
	"/v1/mistakes",
	"/v1/quick-catalog?subject=Physics",
	"/v1/quick-catalog?subject=Chemistry",
	"/v1/quick-catalog?subject=Mathematics",
	"/v1/quick-exams",
}

func tokensFrom(body []byte) (access, refresh string) {
	var v struct {
		Tokens struct {
			AccessToken  string `json:"access_token"`
			RefreshToken string `json:"refresh_token"`
		} `json:"tokens"`
	}
	if json.Unmarshal(body, &v) != nil {
		return "", ""
	}
	return v.Tokens.AccessToken, v.Tokens.RefreshToken
}

func getAuth(ctx context.Context, client *http.Client, url, access string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+access)
	res, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	return res.StatusCode, body, err
}

func postJSON(ctx context.Context, client *http.Client, url string, payload any) (int, []byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	return res.StatusCode, body, err
}

func printStats(results []result) {
	by := map[string][]time.Duration{}
	counts := map[string]int{}
	for _, r := range results {
		counts[r.op]++
		if r.err == "" {
			by[r.op] = append(by[r.op], r.lat)
		}
	}
	fmt.Printf("\n%-22s %6s %6s %10s %10s %10s\n", "op", "n", "ok", "p50", "p95", "max")
	ops := make([]string, 0, len(counts))
	for op := range counts {
		ops = append(ops, op)
	}
	sort.Strings(ops)
	for _, op := range ops {
		lats := by[op]
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		fmt.Printf("%-22s %6d %6d %10s %10s %10s\n",
			op, counts[op], len(lats), pct(lats, 0.50), pct(lats, 0.95), pct(lats, 1))
	}
}

func pct(lats []time.Duration, p float64) string {
	if len(lats) == 0 {
		return "-"
	}
	i := int(float64(len(lats)-1) * p)
	return lats[i].Round(time.Millisecond).String()
}

func errString(err error, body []byte) string {
	if err != nil {
		return err.Error()
	}
	return truncate(body)
}

func truncate(b []byte) string {
	s := string(bytes.TrimSpace(b))
	if len(s) > 180 {
		return s[:180]
	}
	return s
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
