module github.com/TicketsBot-cloud/gdpr-worker

go 1.24.0

//replace github.com/TicketsBot-cloud/database => ../database

//replace github.com/TicketsBot-cloud/gdl => ../gdl

replace github.com/TicketsBot-cloud/archiverclient => ./archiverclient

//replace github.com/TicketsBot-cloud/logarchiver => ../logarchiver

require (
	github.com/TicketsBot-cloud/archiverclient v0.0.0-20250514201416-cf23f65eb3fc
	github.com/TicketsBot-cloud/database v0.0.0-20250918212912-4cc263bc1b41
	github.com/TicketsBot-cloud/gdl v0.0.0-20250917180424-569348f7a55b
	github.com/TicketsBot-cloud/logarchiver v0.0.0-20250514201320-d5141071a6eb
	github.com/caarlos0/env/v10 v10.0.0
	github.com/go-redis/redis/v8 v8.11.5
	github.com/jackc/pgx/v4 v4.18.3
	github.com/joho/godotenv v1.5.1
	go.uber.org/zap v1.27.0
)

require (
	github.com/TicketsBot-cloud/common v0.0.0-20250208132851-d5083bb04d98 // indirect
	github.com/TicketsBot/common v0.0.0-20241104184641-e39c64bdcf3e // indirect
	github.com/TicketsBot/ttlcache v1.6.1-0.20200405150101-acc18e37b261 // indirect
	github.com/caarlos0/env v3.5.0+incompatible // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/goccy/go-json v0.10.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.14.3 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.3 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/pgtype v1.14.0 // indirect
	github.com/jackc/pgx v3.6.2+incompatible // indirect
	github.com/jackc/pgx/v5 v5.6.0 // indirect
	github.com/jackc/puddle v1.3.0 // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/juju/ratelimit v1.0.1 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/klauspost/cpuid/v2 v2.2.8 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/minio-go/v7 v7.0.73 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pasztorpisti/qs v0.0.0-20171216220353-8d6c33ee906c // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/rs/xid v1.5.0 // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/crypto v0.28.0 // indirect
	golang.org/x/exp v0.0.0-20241108190413-2d47ceb2692f // indirect
	golang.org/x/net v0.26.0 // indirect
	golang.org/x/sync v0.9.0 // indirect
	golang.org/x/sys v0.26.0 // indirect
	golang.org/x/text v0.19.0 // indirect
)
