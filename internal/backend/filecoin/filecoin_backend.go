package filecoin

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"path/filepath"

	ipfspath "github.com/ipfs/interface-go-ipfs-core/path"
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
	config Config
	backend.Layout
}

type Config struct {
	Token            string
	ServerAddr       string
	IPFSRevProxyAddr string
	Layout           string `option:"layout" help:"use this backend layout (default: auto-detect)"`
	BackupPath       string
	BackupUniqueID   string
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
	fmt.Println("open called")

	be, err := New(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "creating filecoin client")
	}

	be.Layout, err = backend.ParseLayout(ctx, be, cfg.Layout, defaultLayout, cfg.BackupPath)
	if err != nil {
		return nil, errors.Wrap(err, "parsing layout")
	}

	return be, nil
}

func Create(ctx context.Context, cfg Config) (*FilecoinBackend, error) {
	debug.Log("Create() called\n")

	be, err := New(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "creating filecoin client")
	}

	be.Layout, err = backend.ParseLayout(ctx, be, cfg.Layout, defaultLayout, cfg.BackupPath)
	if err != nil {
		return nil, errors.Wrap(err, "parsing layout")
	}

	authnCtx := setAuthCtx(ctx, cfg.Token)

	if os.Getenv("FILECOIN_BYPASS_INIT_CHECK") == "" {

		ipfs, err := newDecoratedIPFSAPI(cfg.IPFSRevProxyAddr, cfg.Token)
		if err != nil {
			return nil, errors.Wrap(err, "creating ipfs API")
		}

		p := ipfspath.New(cfg.BackupUniqueID)

		debug.Log("ipfspath = %v\n", p.String())

		_, err = ipfs.Unixfs().Ls(authnCtx, p)
		if err != nil {
			if !strings.HasPrefix(err.Error(), "no link named") {
				return nil, errors.Wrap(err, "listing files")
			}
		} else {
			return nil, errors.New("repository already initialized")
		}

	}
	// if alreadyExists {
	// 	fmt.Printf("folder already existed\n")
	// 	for dirEntry := range dirEntriesChan {
	// 		fmt.Printf("dirEntry = %+v\n", dirEntry)
	// 	}
	// }

	// TODO: should check if initial dir does not already exist

	tmpInitDir, err := ioutil.TempDir("", "restic-filecoin")
	if err != nil {
		return nil, errors.Wrap(err, "creating tempdir")
	}
	defer func() {
		os.RemoveAll(tmpInitDir)
	}()

	for _, d := range be.Paths() {
		absPath := filepath.Join(tmpInitDir, d)
		if err := os.MkdirAll(absPath, 0700); err != nil {
			return nil, errors.Wrapf(err, "creating initial directory %q", absPath)
		}
	}

	cid, err := be.client.Data.StageFolder(authnCtx, cfg.IPFSRevProxyAddr, tmpInitDir)
	if err != nil {
		return nil, errors.Wrap(err, "staging initial directory layout")
	}

	fmt.Printf("folder staged with CID: %q\n", cid)

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
		config: cfg,
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
	debug.Log("Test(%+v)", h)

	ipfs, err := newDecoratedIPFSAPI(be.config.IPFSRevProxyAddr, be.config.Token)
	if err != nil {
		return false, errors.Wrap(err, "creating ipfs API")
	}

	p := ipfspath.New(filepath.Join(be.config.BackupUniqueID, be.Filename(h)))

	authnCtx := setAuthCtx(ctx, be.config.Token)
	withTimeout, cancelFunc := context.WithTimeout(authnCtx, 3*time.Second)
	defer cancelFunc()

	debug.Log("ipfspath = %v\n", p.String())

	stat, err := ipfs.Block().Stat(withTimeout, p)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return false, nil
		}
		return false, errors.Wrap(err, "Get()")
	}

	debug.Log("stat = %+v", stat)

	return true, nil
}

// IsNotExist returns true if the file does not exist.
func (be *FilecoinBackend) IsNotExist(err error) bool {
	debug.Log("IsNotExist()\n")
	return errors.Cause(err) == errNotFound
}

// Save adds new Data to the backend.
func (be *FilecoinBackend) Save(ctx context.Context, h restic.Handle, rd restic.
	RewindReader) error {
	debug.Log("Save()\n")

	return ctx.Err()
}

// Load runs fn with a reader that yields the contents of the file at h at the
// given offset.
func (be *FilecoinBackend) Load(ctx context.Context, h restic.Handle, length int, offset int64, fn func(rd io.Reader) error) error {
	debug.Log("Load()\n")

	return backend.DefaultLoad(ctx, h, length, offset, be.openReader, fn)
}

func (be *FilecoinBackend) openReader(ctx context.Context, h restic.Handle, length int, offset int64) (io.ReadCloser, error) {
	debug.Log("openReader()\n")

	return ioutil.NopCloser(nil), nil
}

// Stat returns information about a file in the backend.
func (be *FilecoinBackend) Stat(ctx context.Context, h restic.Handle) (restic.FileInfo, error) {
	debug.Log("Stat()\n")

	return restic.FileInfo{Size: int64(42), Name: "foo"}, ctx.Err()
}

// Remove deletes a file from the backend.
func (be *FilecoinBackend) Remove(ctx context.Context, h restic.Handle) error {
	debug.Log("Remove()\n")

	return ctx.Err()
}

// List returns a channel which yields entries from the backend.
func (be *FilecoinBackend) List(ctx context.Context, t restic.FileType, fn func(restic.FileInfo) error) error {
	debug.Log("List %v", t)

	ipfs, err := newDecoratedIPFSAPI(be.config.IPFSRevProxyAddr, be.config.Token)
	if err != nil {
		return errors.Wrap(err, "creating ipfs API")
	}

	p := ipfspath.New(be.config.BackupUniqueID)

	debug.Log("ipfspath = %v\n", p.String())

	authnCtx := setAuthCtx(ctx, be.config.Token)

	dirEntries, err := ipfs.Unixfs().Ls(authnCtx, p)
	if err != nil {
		if !strings.HasPrefix(err.Error(), "no link named") {
			return errors.Wrap(err, "listing files")
		}
	}

	for dirEntry := range dirEntries {
		debug.Log("send %v\n", dirEntry)

		rfi := restic.FileInfo{}
		if err := fn(rfi); err != nil {
			return errors.Wrapf(err, "executing fn with %v", dirEntry)
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	return ctx.Err()
}

// Location returns the location of the backend (RAM).
func (be *FilecoinBackend) Location() string {
	debug.Log("Location()\n")

	return "filecoin"
}

// Delete removes all data in the backend.
func (be *FilecoinBackend) Delete(ctx context.Context) error {
	debug.Log("Delete()\n")

	return nil
}

// Close closes the backend.
func (be *FilecoinBackend) Close() error {
	debug.Log("Close()\n")

	return nil
}
