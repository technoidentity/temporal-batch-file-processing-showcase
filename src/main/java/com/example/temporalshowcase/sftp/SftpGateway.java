package com.example.temporalshowcase.sftp;

import com.example.temporalshowcase.config.TemporalProperties;
import lombok.extern.slf4j.Slf4j;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Thin SFTP client (list + GET) used by the Pattern-1 poller. Connection
 * settings come from {@link TemporalProperties.Sftp} (application.yml).
 */
@Slf4j
public class SftpGateway {

    private final TemporalProperties.Sftp cfg;

    public SftpGateway(TemporalProperties.Sftp cfg) {
        this.cfg = cfg;
    }

    /** Lists regular-file names in the configured remote directory. */
    public List<String> listRemoteFiles() throws Exception {
        try (SSHClient ssh = connect(); SFTPClient sftp = ssh.newSFTPClient()) {
            List<String> names = new ArrayList<>();
            for (RemoteResourceInfo r : sftp.ls(cfg.getRemoteDir())) {
                // regular files only; skip hidden/dotfiles (e.g. .gitkeep)
                if (r.isRegularFile() && !r.getName().startsWith(".")) {
                    names.add(r.getName());
                }
            }
            return names;
        }
    }

    /** SFTP GET: downloads {@code remoteName} into the local download dir; returns the local path. */
    public Path download(String remoteName) throws Exception {
        Path local = Paths.get(cfg.getDownloadDir(), remoteName);
        if (local.getParent() != null) {
            Files.createDirectories(local.getParent());
        }
        try (SSHClient ssh = connect(); SFTPClient sftp = ssh.newSFTPClient()) {
            sftp.get(remotePath(remoteName), local.toString());
        }
        log.info("SFTP GET {} -> {}", remotePath(remoteName), local);
        return local;
    }

    /** Remote file size in bytes (SFTP stat). */
    public long size(String remoteName) throws Exception {
        try (SSHClient ssh = connect(); SFTPClient sftp = ssh.newSFTPClient()) {
            return sftp.size(remotePath(remoteName));
        }
    }

    /**
     * Resumable byte-range SFTP GET: reads up to {@code length} bytes starting at
     * {@code offset} from the remote file and writes them into {@code localFile} at
     * the same offset. Returns bytes read (0 at EOF). This is what makes the
     * Pattern-8 resumable transfer continue from a checkpoint rather than restart.
     */
    public int downloadSegment(String remoteName, long offset, int length, Path localFile) throws Exception {
        if (localFile.getParent() != null) {
            Files.createDirectories(localFile.getParent());
        }
        try (SSHClient ssh = connect();
             SFTPClient sftp = ssh.newSFTPClient();
             RemoteFile rf = sftp.open(remotePath(remoteName));
             FileChannel out = FileChannel.open(localFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            byte[] buf = new byte[length];
            int n = rf.read(offset, buf, 0, length); // -1 at EOF
            if (n <= 0) {
                return 0;
            }
            out.position(offset);
            out.write(ByteBuffer.wrap(buf, 0, n));
            return n;
        }
    }

    private String remotePath(String remoteName) {
        String d = cfg.getRemoteDir();
        return d.endsWith("/") ? d + remoteName : d + "/" + remoteName;
    }

    private SSHClient connect() throws Exception {
        SSHClient ssh = new SSHClient();
        // Demo: skip host-key verification. In production, pin the server's host key.
        ssh.addHostKeyVerifier(new PromiscuousVerifier());
        ssh.connect(cfg.getHost(), cfg.getPort());
        ssh.authPassword(cfg.getUsername(), cfg.getPassword());
        return ssh;
    }
}
