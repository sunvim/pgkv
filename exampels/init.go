package main

import (
	"log/slog"
	"os"
	"strings"
)

var logname = "examples/"

func Init() {
	opts := slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.SourceKey {
				source := a.Value.Any().(*slog.Source)
				if idx := strings.Index(source.File, logname); idx != -1 {
					source.File = source.File[idx+len(logname):]
				}
				return slog.Any(slog.SourceKey, source)
			}
			return a
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &opts))
	slog.SetDefault(logger)
}
