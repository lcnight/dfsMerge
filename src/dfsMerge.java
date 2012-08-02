import java.util.*;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileInputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.CompressionCodec;

public class dfsMerge extends Configured implements Tool {
    private HashMap<String, String> codecClass = new HashMap<String, String>();
    private HashMap<String, String> codecSuffix = new HashMap<String, String>();

    public dfsMerge() {
        codecClass.put("default", "org.apache.hadoop.io.compress.BZip2Codec");
        codecClass.put("deflate", "org.apache.hadoop.io.compress.DefaultCodec");
        codecClass.put("gzip", "org.apache.hadoop.io.compress.GzipCodec");
        codecClass.put("bzip2", "org.apache.hadoop.io.compress.BZip2Codec");
        codecClass.put("lzo", "com.hadoop.compression.lzo.lzopCodec");

        codecSuffix.put("default", ".bz2");
        codecSuffix.put("deflate", ".deflate");
        codecSuffix.put("gzip", ".gz");
        codecSuffix.put("bzip2", ".bz2");
        codecSuffix.put("lzo", ".lzo");
    }

    public int run(String[] args) throws Exception {
        if (args.length == 0) Usage(); 

        boolean verbose = false;
        boolean compress = false;
        String compresstype = "bzip2";
        String[] srcfiles = null;
        String[] dstfiles = null;
        String dstdir = null;
        for (int i=0 ; i < args.length ; ++i) {
            char c = args[i].charAt(0);
            if (c != '-') { Usage(); }
            c = args[i].charAt(1);
            switch (c)
            {
                case 'm' :
                    srcfiles = args[++i].split(","); break;
                case 'f' :
                    dstfiles = args[++i].split(","); break;
                case 'd' :
                    dstdir = args[++i]; break;
                case 'c':
                    compress = true; break;
                case 't' :
                    compresstype = args[++i]; break;
                case 'v':
                    verbose = true; break;
                case 'h':
                    Usage();
                default :
                    System.err.printf("unknown option [ %c ]\n", c);
                    Usage(); break;
            }  /* end of switch */
        }

        // build dst files (comma separated)
        if ((dstdir != null && dstfiles != null) || (dstdir == null && dstfiles == null)) {
            System.err.println("[-f] and [-d] options cannot be used or not used at the same time\n");
            Usage();
        } else if (dstdir != null && dstfiles == null) {
            dstfiles = new String[srcfiles.length];
            for (int i = 0 ; i < srcfiles.length ; ++i) {
                File f = new File(srcfiles[i]);
                dstfiles[i] = String.format("%s/%s", dstdir, f.getName());
            }
        } else {
        }

        //MiscUtil.printArray(srcfiles);
        //MiscUtil.printArray(dstfiles);

        Configuration conf = getConf();

        // check if in compress mode
        CompressionCodec codec = null;
        Compressor compressor = null;
        if (compress) {
            // create codec
            String comCodecName = codecClass.get(compresstype);
            if (verbose) {
                System.out.printf("----- <summary> running in [%s] compression mode -----\n"+
                        "compression class: %s ,",compresstype, comCodecName);
            }
            Class<?> codecClass = Class.forName(comCodecName);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

            // reform file path with correct suffix
            String comCodecSuffix = codecSuffix.get(compresstype);
            if (verbose) {
                System.out.printf(" suffix: %s\n", comCodecSuffix);
            }
            for (int i=0 ; i < dstfiles.length ; ++i) {
               dstfiles[i] = dstfiles[i] + comCodecSuffix;
            }
        }

        FileSystem fs = FileSystem.get(conf);
        int srccnt = srcfiles.length;
        int dstcnt = dstfiles.length;
        int mcnt = srccnt < dstcnt ? srccnt : dstcnt;
        for (int i = 0 ; i < mcnt ; ++i) {
            File inf = new File(srcfiles[i]);
            Path outpath = new Path(dstfiles[i]);
            if (!inf.exists()) {
                System.err.printf("not exists and skip src [%s]\n", srcfiles[i]);
                continue;
            }

            //InputStream instream = new BufferedInputStream(new FileInputStream(srcfiles[i]));
            InputStream instream = new FileInputStream(srcfiles[i]);
            OutputStream outstream = null;
            if (fs.exists(outpath)) {
                FileStatus outf = fs.getFileStatus(new Path(dstfiles[i]));
                long srclen = inf.length();
                long dstlen = outf.getLen();
                if (srclen <= dstlen) {
                    System.err.printf("src [%s] length %d <= dst [%s] length %d\n", 
                            srcfiles[1], srclen, dstfiles[i], dstlen);
                    continue;
                }
                outstream = fs.append(outpath);
                instream.skip(dstlen);
            } else {
                // real type is <FSDataOutputStream>
                outstream = fs.create(outpath);
            }

            if (verbose) {
                System.out.printf("[%s] merge: %s => %s\n", DateUtil.getCurStr(), 
                        srcfiles[i], dstfiles[i]);
            }
            if (compress) {
                compressor = CodecPool.getCompressor(codec);
                outstream = codec.createOutputStream(outstream, compressor);
            }

            IOUtils.copyBytes(instream, outstream, 4096, true);

            CodecPool.returnCompressor(compressor);

        }
        if (verbose) {
            System.out.printf("[%s] process all files done\n", DateUtil.getCurStr());
        }
       
        if (verbose && (srccnt != dstcnt)) {
            System.err.printf("src files count [%d] != dst files count [%d]\n", 
                    srccnt, dstcnt);
        }
        return 0;
    }

    private void mergeFile(FileSystem fs, InputStream in, OutputStream out) throws Exception {
        IOUtils.copyBytes(in, out, 4096, true);
    }

    public static void main(String[] args) throws Exception
    {
        int ret = ToolRunner.run(new Configuration(), new dfsMerge(), args);
        System.exit(ret);
    }

    public static void Usage() {
        System.out.printf("Usage: <jarClass> [options]\n" +
                "options: \n" +
                "\t-m       src, one file or batch files(comma seperated) need to merge to dfs\n" +
                "\t-f       dst, one file or batch files(comma seperated) match the target for src files\n" +
                "\t-d       dst dir, one directory uri specify target dfs, cannot appear with [-f] options\n" +
                "\t-c       denote merge log file running in compression mode\n" +
                "\t-t       explicitly denote compression type, default is [bzip2], [-c] option must be set\n" +
                "\t-h       show help info and exit\n" +
                "\t-v       run in VERBOSE mode\n\n" 
                );
        System.exit(0);
    }
}
