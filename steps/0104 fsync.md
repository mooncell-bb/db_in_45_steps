File.Write() 方法写入数据时，不会直接映射到磁盘写入，因为操作系统拥有一个内存缓存。写入操作首先进入缓存，随后才同步到磁盘，这种机制可以保证合并重复写入并提升吞吐量。

为确保数据写入磁盘，必须刷新所有缓存层并等待完成。Linux 系统中可以使用 fsync() 系统调用实现刷新磁盘，Go 语言提供了 File.Sync() 方法来同步文件。

fsync() 系统调用只确保文件数据被写入，但不保证文件本身存在，这是因为文件由其父目录记录。如果在断电前执行了创建文件目录操作，但是尚未写入磁盘，数据被写入也无法访问。要解决此问题，需要在上级目录上也使用 fsync() 系统调用。

创建文件、重命名文件以及删除文件都需要对包含目录执行 fsync() 系统调用。Windows 系统不需要此操作，Go 标准库也没有提供同步目录的方法，因此必须直接使用 Linux 系统调用方法，而不能依赖于 Go 提供的 File.Sync() 方法。

storage 目录下创建 os_other.go 和 os_unix.go 文件，创建 createFileSync() 函数，Unix 系统直接使用系统调用，其它系统则使用 Go 提供方法。可以使用 //go:build unix 和 //go:build !unix 来在不同系统上分别构建系统。

```
storage         
├─ kv.go        
├─ kv_entry.go  
├─ kv_test.go   
├─ log.go       
├─ os_other.go  
└─ os_unix.go   
```

os_other.go：

```go
//go:build !unix

package storage

import "os"

func createFileSync(file string) (*os.File, error) {
	return os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0o644)
}
```

os_unix.go：

```go
//go:build unix

package storage

import (
	"os"
	"path"
	"syscall"
)

func createFileSync(file string) (*os.File, error) {
	fp, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0o644)

	if err != nil {
		return nil, err
	}

	if err = syncDir(path.Base(file)); err != nil {
		_ = fp.Close()
		return nil, err
	}

	return fp, err
}

func syncDir(file string) error {
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirfd, err := syscall.Open(path.Dir(file), flags, 0o644)

	if err != nil {
		return err
	}
	
	defer syscall.Close(dirfd)
	return syscall.Fsync(dirfd)
}
```

Log.Open() 方法使用 createFileSync() 函数创建文件，Log.Write() 方法使用 File.Sync() 方法同步修改：

```go
func (log *Log) Open() (err error) {
	log.fp, err = createFileSync(log.FileName)
	return err
}
```

