package filecoin

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"
	powclient "github.com/textileio/powergate/api/client"
)

type memMap map[restic.Handle][]byte

// make sure that MemoryBackend implements backend.Backend
var _ restic.Backend = &FilecoinBackend{}

var errNotFound = errors.New("not found")

type FilecoinBackend struct {
	sem    *backend.Semaphore
	client *powclient.Client
	backend.Layout
}

type Config struct {
	Token            string
	ServerAddr       string
	IPFSRevProxyAddr string
	Layout           string `option:"layout" help:"use this backend layout (default: auto-detect)"`
	BackupPath       string
}

const defaultLayout = "default"

func ParseConfig(s string) (interface{}, error) {
	return Config{}, nil
}

func StripPassword(s string) string {
	return s
}

func setAuthCtx(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, powclient.AuthKey, token)
}

func Open(ctx context.Context, cfg Config) (*FilecoinBackend, error) {
	return New(cfg)
}

func Create(ctx context.Context, cfg Config) (*FilecoinBackend, error) {
	be, err := New(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "creating filecoin client")
	}

	be.Layout, err = backend.ParseLayout(ctx, be, cfg.Layout, defaultLayout, cfg.BackupPath)
	if err != nil {
		return nil, errors.Wrap(err, "parsing layout")
	}

	// ipfs, err := newDecoratedIPFSAPI(cfg.IPFSRevProxyAddr, cfg.Token)
	// if err != nil {
	// 	return nil, errors.Wrap(err, "creating ipfs API")
	// }

	// TODO: should check if initial dir does not already exist

	tmpInitDir, err := ioutil.TempDir("", "restic-filecoin")
	if err != nil {
		return nil, errors.Wrap(err, "creating tempdir")
	}

	for _, d := range be.Paths() {
		absPath := filepath.Join(tmpInitDir, d)
		if err := os.MkdirAll(absPath, 0700); err != nil {
			return nil, errors.Wrapf(err, "creating initial directory %q", absPath)
		}
	}

	// ff, err := newSerialFileFromLocalDir(tmpInitDir)
	// if err != nil {
	// 	return nil, errors.Wrap(err, "initializing file from dir")
	// }

	// defer func() { _ = ff.Close() }()

	// opts := []options.UnixfsAddOption{
	// 	options.Unixfs.CidVersion(1),
	// 	options.Unixfs.Pin(true),
	// }

	// pth, err := ipfs.Unixfs().Add(ctx, files.ToDir(ff), opts...)
	// if err != nil {
	// 	return nil, err
	// }

	authnCtx := setAuthCtx(ctx, cfg.Token)

	cid, err := be.client.Data.StageFolder(authnCtx, cfg.IPFSRevProxyAddr, tmpInitDir)
	if err != nil {
		return nil, errors.Wrap(err, "staging initial directory layout")
	}

	debug.Log("folder staged with CID: %q\n", cid)

	// TODO: Support all apply options
	applyOptions := []powclient.ApplyOption{
		powclient.WithOverride(true),
	}

	resp, err := be.client.StorageConfig.Apply(authnCtx, cid, applyOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "applying initial storage configuration")
	}

	debug.Log("storage config applied with jobID %s\n", resp.JobId)

	return be, err
}

// New returns a new backend that saves all data in a map in memory.
func New(cfg Config) (*FilecoinBackend, error) {
	client, err := powclient.NewClient(cfg.ServerAddr)
	return &FilecoinBackend{
		client: client,
	}, err
}

// Join combines path components with slashes.
func (be *FilecoinBackend) Join(p ...string) string {
	return path.Join(p...)
}

// ReadDir returns the entries for a directory.
func (be *FilecoinBackend) ReadDir(ctx context.Context, dir string) (list []os.FileInfo, err error) {
	debug.Log("ReadDir(%v)", dir)
	return []os.FileInfo{}, nil
}

// Test returns whether a file exists.
func (be *FilecoinBackend) Test(ctx context.Context, h restic.Handle) (bool, error) {
	return false, nil
}

// IsNotExist returns true if the file does not exist.
func (be *FilecoinBackend) IsNotExist(err error) bool {
	return errors.Cause(err) == errNotFound
}

// Save adds new Data to the backend.
func (be *FilecoinBackend) Save(ctx context.Context, h restic.Handle, rd restic.RewindReader) error {
	return ctx.Err()
}

// Load runs fn with a reader that yields the contents of the file at h at the
// given offset.
func (be *FilecoinBackend) Load(ctx context.Context, h restic.Handle, length int, offset int64, fn func(rd io.Reader) error) error {
	return backend.DefaultLoad(ctx, h, length, offset, be.openReader, fn)
}

func (be *FilecoinBackend) openReader(ctx context.Context, h restic.Handle, length int, offset int64) (io.ReadCloser, error) {
	return ioutil.NopCloser(nil), nil
}

// Stat returns information about a file in the backend.
func (be *FilecoinBackend) Stat(ctx context.Context, h restic.Handle) (restic.FileInfo, error) {
	return restic.FileInfo{Size: int64(42), Name: "foo"}, ctx.Err()
}

// Remove deletes a file from the backend.
func (be *FilecoinBackend) Remove(ctx context.Context, h restic.Handle) error {
	return ctx.Err()
}

// List returns a channel which yields entries from the backend.
func (be *FilecoinBackend) List(ctx context.Context, t restic.FileType, fn func(restic.FileInfo) error) error {

	return ctx.Err()
}

// Location returns the location of the backend (RAM).
func (be *FilecoinBackend) Location() string {
	return "filecoin"
}

// Delete removes all data in the backend.
func (be *FilecoinBackend) Delete(ctx context.Context) error {
	return nil
}

// Close closes the backend.
func (be *FilecoinBackend) Close() error {
	return nil
}
