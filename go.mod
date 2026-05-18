module github.com/TicketsBot-cloud/gdpr-worker

go 1.25.0

//replace github.com/TicketsBot-cloud/database => ../database

replace github.com/TicketsBot-cloud/gdl => ../gdl

//replace github.com/TicketsBot-cloud/archiverclient => ./archiverclient

replace github.com/TicketsBot-cloud/logarchiver => ../logarchiver

require (
	github.com/TicketsBot-cloud/archiverclient v0.0.0-20251015181023-f0b66a074704
	github.com/TicketsBot-cloud/common v0.0.0-20260210203202-54154661338e
	github.com/TicketsBot-cloud/database v0.0.0-20260308193919-30a698fefa8b
	github.com/TicketsBot-cloud/gdl v0.0.0-20260306134952-cccb0116fef6
	github.com/TicketsBot-cloud/logarchiver v0.0.0-20250809082842-70aa389bcbdf
	github.com/caarlos0/env/v10 v10.0.0
	github.com/go-redis/redis/v8 v8.11.5
	github.com/jackc/pgx/v4 v4.18.3
	github.com/joho/godotenv v1.5.1
	github.com/minio/minio-go/v7 v7.0.99
	go.uber.org/zap v1.27.1
	golang.org/x/sync v0.20.0
)

require (
	github.com/TicketsBot/common v0.0.0-20241117150316-ff54c97b45c1 // indirect
	github.com/TicketsBot/ttlcache v1.6.1-0.20200405150101-acc18e37b261 // indirect
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/caarlos0/env v3.5.0+incompatible // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.14.3 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.3 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgtype v1.14.4 // indirect
	github.com/jackc/pgx v3.6.2+incompatible // indirect
	github.com/jackc/pgx/v5 v5.9.1 // indirect
	github.com/jackc/puddle v1.3.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/juju/ratelimit v1.0.1 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/crc32 v1.3.0 // indirect
	github.com/minio/crc64nvme v1.1.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pasztorpisti/qs v0.0.0-20171216220353-8d6c33ee906c // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	github.com/tinylib/msgp v1.6.3 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/crypto v0.49.0 // indirect
	golang.org/x/exp v0.0.0-20241108190413-2d47ceb2692f // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.35.0 // indirect
)
