package s3

type Config struct {
	URL          string
	Region       string
	KeyID        string
	KeySecret    string
	Bucket       string
	StorageClass string

	Insecure bool
}
