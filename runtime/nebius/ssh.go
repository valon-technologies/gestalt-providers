package nebius

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type localForwarder struct {
	listener  net.Listener
	network   string
	address   string
	remote    string
	client    *ssh.Client
	closeOnce sync.Once
}

func waitForSSH(ctx context.Context, host, user string, signer ssh.Signer, hostKey ssh.PublicKey, timeout time.Duration) (*ssh.Client, error) {
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var lastErr error
	for {
		client, err := ssh.Dial("tcp", net.JoinHostPort(host, fmt.Sprintf("%d", sshPort)), &ssh.ClientConfig{
			User:            user,
			Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
			HostKeyCallback: ssh.FixedHostKey(hostKey),
			Timeout:         5 * time.Second,
		})
		if err == nil {
			return client, nil
		}
		lastErr = err
		select {
		case <-deadlineCtx.Done():
			return nil, fmt.Errorf("wait for ssh on %s: %w", host, lastErr)
		case <-time.After(2 * time.Second):
		}
	}
}

func waitForBootstrap(ctx context.Context, client *ssh.Client, timeout time.Duration) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	const command = "sh -lc 'cloud-init status --wait >/dev/null 2>&1 || true; sudo docker version >/dev/null 2>&1'"
	var lastErr error
	for {
		if _, err := runRemoteCommand(deadlineCtx, client, command); err == nil {
			return nil
		} else {
			lastErr = err
		}
		select {
		case <-deadlineCtx.Done():
			return fmt.Errorf("wait for cloud-init/docker bootstrap: %w", lastErr)
		case <-time.After(2 * time.Second):
		}
	}
}

func runRemoteCommand(ctx context.Context, client *ssh.Client, command string) (string, error) {
	if client == nil {
		return "", fmt.Errorf("ssh client is not configured")
	}
	session, err := client.NewSession()
	if err != nil {
		return "", err
	}
	defer session.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	session.Stdout = &stdout
	session.Stderr = &stderr
	if err := session.Start(command); err != nil {
		return "", err
	}

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- session.Wait()
	}()

	select {
	case err := <-waitCh:
		output := strings.TrimSpace(stdout.String())
		if err != nil {
			errText := strings.TrimSpace(stderr.String())
			if errText == "" {
				errText = strings.TrimSpace(stdout.String())
			}
			if errText != "" {
				return output, fmt.Errorf("%w: %s", err, errText)
			}
			return output, err
		}
		return output, nil
	case <-ctx.Done():
		_ = session.Close()
		<-waitCh
		return "", ctx.Err()
	}
}

func uploadBundleDir(ctx context.Context, client *ssh.Client, localDir, remoteDir string) error {
	session, err := client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()

	stdin, err := session.StdinPipe()
	if err != nil {
		return err
	}
	var stderr bytes.Buffer
	session.Stderr = &stderr
	command := "mkdir -p " + shellQuote(remoteDir) + " && tar -xzf - -C " + shellQuote(remoteDir)
	if err := session.Start(command); err != nil {
		return err
	}

	writeErrCh := make(chan error, 1)
	go func() {
		writeErrCh <- writeTarGz(stdin, localDir)
		_ = stdin.Close()
	}()

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- session.Wait()
	}()

	select {
	case err := <-waitCh:
		writeErr := <-writeErrCh
		if writeErr != nil {
			return writeErr
		}
		if err != nil {
			return fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
		}
		return nil
	case <-ctx.Done():
		_ = session.Close()
		<-waitCh
		<-writeErrCh
		return ctx.Err()
	}
}

func writeTarGz(w io.Writer, root string) error {
	gzw := gzip.NewWriter(w)
	tw := tar.NewWriter(gzw)

	err := filepath.Walk(root, func(localPath string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, localPath)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		rel = filepath.ToSlash(rel)

		linkTarget := ""
		if info.Mode()&os.ModeSymlink != 0 {
			linkTarget, err = os.Readlink(localPath)
			if err != nil {
				return err
			}
		}
		header, err := tar.FileInfoHeader(info, linkTarget)
		if err != nil {
			return err
		}
		header.Name = rel
		if info.IsDir() && !strings.HasSuffix(header.Name, "/") {
			header.Name += "/"
		}
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			file, err := os.Open(localPath)
			if err != nil {
				return err
			}
			if _, err := io.Copy(tw, file); err != nil {
				_ = file.Close()
				return err
			}
			if err := file.Close(); err != nil {
				return err
			}
		}
		return nil
	})
	closeErr := tw.Close()
	gzipErr := gzw.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return gzipErr
}

func newLocalForwarder(client *ssh.Client, remoteHost string, remotePort int) (*localForwarder, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	f := &localForwarder{
		listener: listener,
		network:  "tcp",
		address:  listener.Addr().String(),
		remote:   net.JoinHostPort(remoteHost, fmt.Sprintf("%d", remotePort)),
		client:   client,
	}
	go f.serve()
	return f, nil
}

func (f *localForwarder) DialTarget() string {
	return "tcp://" + f.address
}

func (f *localForwarder) Close() error {
	if f == nil || f.listener == nil {
		return nil
	}
	var err error
	f.closeOnce.Do(func() {
		err = f.listener.Close()
	})
	return err
}

func (f *localForwarder) serve() {
	for {
		conn, err := f.listener.Accept()
		if err != nil {
			return
		}
		go f.handle(conn)
	}
}

func (f *localForwarder) handle(localConn net.Conn) {
	defer localConn.Close()

	remoteConn, err := f.client.Dial(f.network, f.remote)
	if err != nil {
		return
	}
	defer remoteConn.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(remoteConn, localConn)
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(localConn, remoteConn)
	}()
	wg.Wait()
}

func waitForPluginReady(ctx context.Context, dialTarget string) error {
	network, address, err := parseLocalDialTarget(dialTarget)
	if err != nil {
		return err
	}

	var lastErr error
	for {
		conn, err := grpc.NewClient(
			address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, address)
			}),
		)
		if err == nil {
			client := proto.NewProviderLifecycleClient(conn)
			callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_, rpcErr := client.GetProviderIdentity(callCtx, &emptypb.Empty{})
			cancel()
			_ = conn.Close()
			if rpcErr == nil {
				return nil
			}
			lastErr = rpcErr
		} else {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("connect to hosted plugin %s: %w", dialTarget, lastErr)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func parseLocalDialTarget(target string) (string, string, error) {
	target = strings.TrimSpace(target)
	if strings.HasPrefix(target, "tcp://") {
		address := strings.TrimSpace(strings.TrimPrefix(target, "tcp://"))
		if address == "" {
			return "", "", fmt.Errorf("tcp dial target is missing host:port")
		}
		return "tcp", address, nil
	}
	return "", "", fmt.Errorf("unsupported nebius plugin dial target %q", target)
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}

func joinShellCommand(argv []string) string {
	quoted := make([]string, 0, len(argv))
	for _, arg := range argv {
		quoted = append(quoted, shellQuote(arg))
	}
	return strings.Join(quoted, " ")
}
