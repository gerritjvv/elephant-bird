package com.twitter.elephantbird.mapreduce.input;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.pig.LoadFunc;

import com.hadoop.compression.lzo.LzoIndex;
import com.twitter.elephantbird.util.PathPartitionHelper;

/**
 * Is a wrapper arround other input formats giving them partition filter
 * capabilities.
 * 
 * @param <K>
 * @param <V>
 */
public class LzoTextPartitionFilterInputFormat extends com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat {

	transient PathPartitionHelper partitionHelper = new PathPartitionHelper();
	Class<? extends LoadFunc> loaderClass;
	String signature;
	
	public LzoTextPartitionFilterInputFormat(
			Class<? extends LoadFunc> loaderClass, String signature) {
		super();
		this.loaderClass = loaderClass;
		this.signature = signature;
	}

	
	@Override
	protected List<FileStatus> listStatus(final JobContext ctx)
			throws IOException {

		Collection<FileStatus> files = null;

		try {
			files = partitionHelper.listStatus(ctx, loaderClass, signature,
					new FilenameFilter() {

						@Override
						public boolean accept(File dir, String name) {
							return name.endsWith(".lzo");
						}

					});
		} catch (InterruptedException e) {
			Thread.interrupted();
			return null;
		} catch (ExecutionException excp) {
			throw new RuntimeException(excp);
		}

		if (files == null) {
			files = super.listStatus(ctx);
		} else {

			System.out.println("Listing Indexes");

			// To help split the files at LZO boundaries, walk the list of lzo
			// files and, if they
			// have an associated index file, save that for later.
			
//			Configuration ctxConf = ctx.getConfiguration();
//			for (final FileStatus result : files) {
//				try{
//					Path path = result.getPath();
//					if(path != null){
//					  LzoIndex index = LzoIndex.readIndex(path.getFileSystem(ctxConf), path);
//					  super.addToIndex(result.getPath(), index);
//					}
//				}catch(Throwable t){
//					System.out.println("Result: " + result);
//					System.out.println("ctxConf: " + ctxConf);
//					
//					t.printStackTrace();
//					throw new RuntimeException(t);
//				}
//				
//			}

		}

		System.out.println("Found " + files.size() + " files");
		return new ArrayList<FileStatus>(files);

	}

}
