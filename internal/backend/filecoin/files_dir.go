package filecoin

import (
	"os"

	files "github.com/ipfs/go-ipfs-files"
	"github.com/restic/restic/internal/errors"
)

func newSerialFileFromLocalDir(folderPath string) (files.Node, error) {

	stat, err := os.Lstat(folderPath)
	if err != nil {
		return nil, errors.Wrapf(err, "stating %q", folderPath)
	}

	ff, err := files.NewSerialFile(folderPath, true, stat)
	if err != nil {
		return nil, errors.Wrap(err, "NewSerialFile()")
	}

	return ff, nil
}
