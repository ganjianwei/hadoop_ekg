package org.apache.hadoop.mapred.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.mapred.TaskTrackerStatus;
/**
 * Extension of the LinuxResourceCalculatorPlugin with new features such as:
 *Disk io resource information
 **/

public class LinuxResourceCalculatorExtendedPlugin extends LinuxResourceCalculatorPlugin{
	  private static final Log LOG =
		  LogFactory.getLog(LinuxResourceCalculatorExtendedPlugin.class);
    private static final String PROCFS_DISKSTATSFILE= "/proc/diskstats";
    private String procfsDiskStatsFile;
    
    private static final int UNAVAILABLE = -1;
    private float diskIOUsage = UNAVAILABLE;
    
    private long sampleTime = UNAVAILABLE;
    private long lastSampleTime = UNAVAILABLE;
    private long cumulativeReads = UNAVAILABLE;
    private long lastCumulativeReads = UNAVAILABLE;
    private long cumulativeWrites = UNAVAILABLE;
    private long lastCumulativeWrites = UNAVAILABLE;
    
    private long cumulativeDiskIOTime = UNAVAILABLE;
    
    public LinuxResourceCalculatorExtendedPlugin(){
    	super(); 
    	procfsDiskStatsFile=PROCFS_DISKSTATSFILE;
        
    }
    /**
     * Obtain the cumulative time spent doing I/Os.
     * @return cumulative time spent on IO in ms.
     */
    public long getCumulativeDiskIOTime(){
        readProcDiskStatFile();
        return cumulativeDiskIOTime;
    }
    
    /**
   * Obtain the disk IO Bandwidth of the machine. Return -1 if it is unavailable
   *
   * @return Disk IO Bandwidth usage in IOs/sec
   */
    public float getDiskIOUsage(){
    	 readProcDiskStatFile();
    	    sampleTime = getCurrentTime();
    	    if (lastSampleTime == UNAVAILABLE ||
    	        lastSampleTime > sampleTime) {
    	      // lastSampleTime > sampleTime may happen when the system time is changed
    	      lastSampleTime = sampleTime;
    	      lastCumulativeReads = cumulativeReads;
    	      lastCumulativeWrites = cumulativeWrites;
    	      return diskIOUsage;
    	    }
    	    // When lastSampleTime is sufficiently old, update cpuUsage.
    	    // Also take a sample of the current time and cumulative CPU time for the
    	    // use of the next calculation.
    	    final long MINIMUM_UPDATE_INTERVAL = 10 * jiffyLengthInMillis;
    	    if (sampleTime > lastSampleTime + MINIMUM_UPDATE_INTERVAL) {
    		    diskIOUsage = 1000*(float)((cumulativeReads-lastCumulativeReads) + (cumulativeWrites-lastCumulativeWrites)) /
    		               ((float)(sampleTime - lastSampleTime));
    		    lastSampleTime = sampleTime;
    	      lastCumulativeReads= cumulativeReads;
    	      lastCumulativeWrites = cumulativeWrites;
    	    }
   
    	    return diskIOUsage;
    }


    /**
     * Read the /proc/diskstats file, parse, and calculate cumulative IO
     */
    private void readProcDiskStatFile(){
        BufferedReader in = null;
        FileReader fReader = null;
        try{
            fReader = new FileReader(procfsDiskStatsFile);
            in = new BufferedReader(fReader);
        } catch(FileNotFoundException f){
            // shouldn't happen...
            return;
        }

        Matcher mat = null;
        try{
            String str = in.readLine();
            while(str != null){
            	//mat = DISKSTATS_IO_FORMAT.matcher(str);
            	String[] diskInfo = str.trim().split("[ \t ]+"); 
                if(diskInfo[2].equals("sda")){
                    cumulativeReads  = Long.parseLong(diskInfo[3]);
                    cumulativeWrites = Long.parseLong(diskInfo[7]);
                    cumulativeDiskIOTime = Long.parseLong(diskInfo[13]);
                    break;	
                }
                str = in.readLine();
            }
        }catch(IOException io){
            LOG.warn("Error reading the stream "+ io);
        } finally {
            //close the streams 
            try{
                fReader.close();
                try{
                    in.close();
                }catch(IOException i){
                    LOG.warn("Error closing the stream " + in);
                }
            } catch(IOException i){
                LOG.warn("Error closing the stream "+ fReader);
                }
        }
    }
    public static void main(String[] args) {
        LinuxResourceCalculatorExtendedPlugin plugin = new LinuxResourceCalculatorExtendedPlugin();
        System.out.println("Physical memory Size (bytes) : "
            + plugin.getPhysicalMemorySize());
        System.out.println("Total Virtual memory Size (bytes) : "
            + plugin.getVirtualMemorySize());
        System.out.println("Available Physical memory Size (bytes) : "
            + plugin.getAvailablePhysicalMemorySize());
        System.out.println("Total Available Virtual memory Size (bytes) : "
            + plugin.getAvailableVirtualMemorySize());
        System.out.println("Number of Processors : " + plugin.getNumProcessors());
        System.out.println("CPU frequency (kHz) : " + plugin.getCpuFrequency());
        System.out.println("Cumulative CPU time (ms) : " +
                plugin.getCumulativeCpuTime());
        plugin.getCpuUsage();
        plugin.getDiskIOUsage();
        try {
          // Sleep so we can compute the CPU usage
          Thread.sleep(500L);
        } catch (InterruptedException e) {
          // do nothing
        }
        System.out.println("CPU usage % : " + plugin.getCpuUsage());
        System.out.println("Disk IO Bandwidth % : " + plugin.getDiskIOUsage());
        try {
            // Sleep so we can compute the CPU usage
            Thread.sleep(10000L);
          } catch (InterruptedException e) {
            // do nothing
          }
          System.out.println("CPU usage % : " + plugin.getCpuUsage());
          System.out.println("Disk IO Bandwidth % : " + plugin.getDiskIOUsage());
      }
        
    }
    







