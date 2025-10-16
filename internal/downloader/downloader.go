package downloader

import (
    "bufio"
    "context"
    "io"
    "os/exec"
    "regexp"
    "strconv"
    "strings"
    "time"
)

var percentRe = regexp.MustCompile(`(\d{1,3})%`)

type ProgressFunc func(pct int)

type Config struct {
	YtDLPTimeout         time.Duration
	DownloadTimeout      time.Duration
}

type Downloader struct {
	cfg Config
	sem chan struct{}
}

func New(cfg Config, maxConcurrent int) *Downloader {
	return &Downloader{cfg: cfg, sem: make(chan struct{}, maxConcurrent)}
}

func (d *Downloader) withPermit(fn func() error) error {
	d.sem <- struct{}{}
	defer func(){ <-d.sem }()
	return fn()
}

func (d *Downloader) FetchMetadata(ctx context.Context, url string) (title, thumbnail string, durationSeconds int, err error) {
	// Fast path could use external services; fallback to yt-dlp --dump-json
	ctx, cancel := context.WithTimeout(ctx, d.cfg.YtDLPTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "yt-dlp", "--dump-json", "--no-playlist", url)
	out, err := cmd.Output()
	if err != nil { return "", "", 0, err }
	// naive extraction to avoid adding JSON lib deps here; prefer robust parsing in production
	// Title
	title = extractJSONField(string(out), "title")
	thumbnail = extractJSONField(string(out), "thumbnail")
	durStr := extractJSONField(string(out), "duration")
	if durStr != "" {
		// duration is numeric; attempt to parse
		if strings.ContainsAny(durStr, ".") { durStr = strings.SplitN(durStr, ".", 2)[0] }
		if n, e := strconv.Atoi(durStr); e == nil { durationSeconds = n }
	}
	return
}

func extractJSONField(js, field string) string {
	// very naive; expects "field": value,
	idx := strings.Index(js, "\""+field+"\"")
	if idx == -1 { return "" }
	s := js[idx:]
	colon := strings.Index(s, ":")
	if colon == -1 { return "" }
	v := strings.TrimSpace(s[colon+1:])
	if len(v) == 0 { return "" }
	// remove leading quotes
	if v[0] == '"' {
		v = v[1:]
		end := strings.IndexByte(v, '"')
		if end == -1 { return "" }
		return v[:end]
	}
	// numeric until comma
	end := strings.IndexByte(v, ',')
	if end == -1 { end = len(v) }
	return strings.TrimSpace(v[:end])
}

func (d *Downloader) Download(ctx context.Context, url, outputPath string, onProgress ProgressFunc) error {
	return d.withPermit(func() error {
		ctx, cancel := context.WithTimeout(ctx, d.cfg.DownloadTimeout)
		defer cancel()
        args := []string{"-f", "bestaudio/best", "-o", outputPath, "--no-playlist", "--newline", url}
		cmd := exec.CommandContext(ctx, "yt-dlp", args...)
		stderr, err := cmd.StderrPipe()
		if err != nil { return err }
		stdout, err := cmd.StdoutPipe()
		if err != nil { return err }
		if err := cmd.Start(); err != nil { return err }
		go readProgress(stderr, onProgress)
		go readProgress(stdout, onProgress)
		return cmd.Wait()
	})
}

func readProgress(r io.Reader, onProgress ProgressFunc) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if m := percentRe.FindStringSubmatch(line); len(m) == 2 {
			if p, err := strconv.Atoi(m[1]); err == nil {
				if p < 0 { p = 0 } ; if p > 100 { p = 100 }
				onProgress(p)
			}
		}
	}
}
