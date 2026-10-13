package ports

// SecretBox provides encryption and decryption for sensitive data at rest (e.g. RequestContext secrets).
// Implementations must be safe for concurrent use.
type SecretBox interface {
	// Seal encrypts plain data and returns ciphertext.
	Seal(plain []byte) ([]byte, error)
	// Open decrypts cipher data and returns plaintext.
	Open(cipher []byte) ([]byte, error)
}
