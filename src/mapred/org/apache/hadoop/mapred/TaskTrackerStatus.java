/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import org.apache.hadoop.io.*;

import java.io.*;
import java.util.*;

/**************************************************
 * A TaskTrackerStatus is a MapReduce primitive.  Keeps
 * info on a TaskTracker.  The JobTracker maintains a set
 * of the most recent TaskTrackerStatus objects for each
 * unique TaskTracker it knows about.
 *
 **************************************************/
class TaskTrackerStatus implements Writable {

    static {                                        // register a ctor
        WritableFactories.setFactory
            (TaskTrackerStatus.class,
             new WritableFactory() {
                 public Writable newInstance() { return new TaskTrackerStatus(); }
             });
    }

    String trackerName;
    String host;
    int httpPort;
    int failures;
    List<TaskStatus> taskReports;

    volatile long lastSeen;
    private int maxMapTasks;
    private int maxReduceTasks;

    public static final int UNAVAILABLE = -1;
    /**
     * Class representing a collection of resources on this tasktracker.
     */
    static class ResourceStatus implements Writable {

        private long totalVirtualMemory ;
        private long totalPhysicalMemory;
        private long mapSlotMemorySizeOnTT = UNAVAILABLE;
        private long reduceSlotMemorySizeOnTT = UNAVAILABLE;
        private long availableSpace ;
        private long availableVirtualMemory = UNAVAILABLE;
        private long availablePhysicalMemory = UNAVAILABLE;
        private long cumulativeCpuTime = UNAVAILABLE;
        private long cpuFrequency = UNAVAILABLE;
        private int numProcessors = UNAVAILABLE;
        private float cpuUsage = UNAVAILABLE;
        private float diskIOUsage = UNAVAILABLE;

        ResourceStatus() {
            totalVirtualMemory = JobConf.DISABLED_MEMORY_LIMIT;
            totalPhysicalMemory = JobConf.DISABLED_MEMORY_LIMIT;
            mapSlotMemorySizeOnTT = JobConf.DISABLED_MEMORY_LIMIT;
            reduceSlotMemorySizeOnTT = JobConf.DISABLED_MEMORY_LIMIT;
            availableSpace = Long.MAX_VALUE;
        }

        /**
         * Set the maximum amount of virtual memory on the tasktracker.
         * 
         * @param vmem maximum amount of virtual memory on the tasktracker in bytes.
         */
        void setTotalVirtualMemory(long totalMem) {
            totalVirtualMemory = totalMem;
        }

        /**
         * Get the maximum amount of virtual memory on the tasktracker.
         * 
         * If this is {@link JobConf#DISABLED_MEMORY_LIMIT}, it should be ignored
         * and not used in any computation.
         * 
         * @return the maximum amount of virtual memory on the tasktracker in bytes.
         */
        long getTotalVirtualMemory() {
            return totalVirtualMemory;
        }

        /**
         * Set the maximum amount of physical memory on the tasktracker.
         * 
         * @param totalRAM maximum amount of physical memory on the tasktracker in
         *          bytes.
         */
        void setTotalPhysicalMemory(long totalRAM) {
            totalPhysicalMemory = totalRAM;
        }

        /**
         * Get the maximum amount of physical memory on the tasktracker.
         * 
         * If this is {@link JobConf#DISABLED_MEMORY_LIMIT}, it should be ignored
         * and not used in any computation.
         * 
         * @return maximum amount of physical memory on the tasktracker in bytes.
         */
        long getTotalPhysicalMemory() {
            return totalPhysicalMemory;
        }

        /**
         * Set the memory size of each map slot on this TT. This will be used by JT
         * for accounting more slots for jobs that use more memory.
         * 
         * @param mem
         */
        void setMapSlotMemorySizeOnTT(long mem) {
            mapSlotMemorySizeOnTT = mem;
        }

        /**
         * Get the memory size of each map slot on this TT. See
         * {@link #setMapSlotMemorySizeOnTT(long)}
         * 
         * @return
         */
        long getMapSlotMemorySizeOnTT() {
            return mapSlotMemorySizeOnTT;
        }

        /**
         * Set the memory size of each reduce slot on this TT. This will be used by
         * JT for accounting more slots for jobs that use more memory.
         * 
         * @param mem
         */
        void setReduceSlotMemorySizeOnTT(long mem) {
            reduceSlotMemorySizeOnTT = mem;
        }

        /**
         * Get the memory size of each reduce slot on this TT. See
         * {@link #setReduceSlotMemorySizeOnTT(long)}
         * 
         * @return
         */
        long getReduceSlotMemorySizeOnTT() {
            return reduceSlotMemorySizeOnTT;
        }

        /**
         * Set the available disk space on the TT
         * @param availSpace
         */
        void setAvailableSpace(long availSpace) {
            availableSpace = availSpace;
        }

           /**
     * Set the amount of available virtual memory on the tasktracker.
     * If the input is not a valid number, it will be set to UNAVAILABLE
     *
     * @param vmem amount of available virtual memory on the tasktracker
     *                    in bytes.
     */
    void setAvailableVirtualMemory(long availableMem) {
      availableVirtualMemory = availableMem > 0 ?
                               availableMem : UNAVAILABLE;
    }

    /**
     * Get the amount of available virtual memory on the tasktracker.
     * Will return UNAVAILABLE if it cannot be obtained
     *
     * @return the amount of available virtual memory on the tasktracker
     *             in bytes.
     */
    long getAvailabelVirtualMemory() {
      return availableVirtualMemory;
    }

    /**
     * Set the amount of available physical memory on the tasktracker.
     * If the input is not a valid number, it will be set to UNAVAILABLE
     *
     * @param availableRAM amount of available physical memory on the
     *                     tasktracker in bytes.
     */
    void setAvailablePhysicalMemory(long availableRAM) {
      availablePhysicalMemory = availableRAM > 0 ?
                                availableRAM : UNAVAILABLE;
    }

    /**
     * Get the amount of available physical memory on the tasktracker.
     * Will return UNAVAILABLE if it cannot be obtained
     *
     * @return amount of available physical memory on the tasktracker in bytes.
     */
    long getAvailablePhysicalMemory() {
      return availablePhysicalMemory;
    }
      
        /**
     * Set the CPU frequency of this TaskTracker
     * If the input is not a valid number, it will be set to UNAVAILABLE
     *
     * @param cpuFrequency CPU frequency in kHz
     */
    public void setCpuFrequency(long cpuFrequency) {
      this.cpuFrequency = cpuFrequency > 0 ?
                          cpuFrequency : UNAVAILABLE;
    }

      /**
     * Get the CPU frequency of this TaskTracker
     * Will return UNAVAILABLE if it cannot be obtained
     *
     * @return CPU frequency in kHz
     */
    public long getCpuFrequency() {
      return cpuFrequency;
    }

    /**
     * Set the number of processors on this TaskTracker
     * If the input is not a valid number, it will be set to UNAVAILABLE
     *
     * @param numProcessors number of processors
     */
    public void setNumProcessors(int numProcessors) {
      this.numProcessors = numProcessors > 0 ?
                           numProcessors : UNAVAILABLE;
    }

    /**
     * Get the number of processors on this TaskTracker
     * Will return UNAVAILABLE if it cannot be obtained
     *
     * @return number of processors
     */
    public int getNumProcessors() {
      return numProcessors;
    }

    /**
     * Set the cumulative CPU time on this TaskTracker since it is up
     * It can be set to UNAVAILABLE if it is currently unavailable.
     *
     * @param cumulativeCpuTime Used CPU time in millisecond
     */
    public void setCumulativeCpuTime(long cumulativeCpuTime) {
      this.cumulativeCpuTime = cumulativeCpuTime > 0 ?
                               cumulativeCpuTime : UNAVAILABLE;
    }

    /**
     * Get the cumulative CPU time on this TaskTracker since it is up
     * Will return UNAVAILABLE if it cannot be obtained
     *
     * @return used CPU time in milliseconds
     */
    public long getCumulativeCpuTime() {
      return cumulativeCpuTime;
    }
    
    /**
     * Set the CPU usage on this TaskTracker
     * 
     * @param cpuUsage CPU usage in %
     */
    public void setCpuUsage(float cpuUsage) {
      this.cpuUsage = cpuUsage;
    }

    /**
     * Get the CPU usage on this TaskTracker
     * Will return UNAVAILABLE if it cannot be obtained
     *
     * @return CPU usage in %
     */
    public float getCpuUsage() {
      return cpuUsage;
    } 
       
        public void setDiskIOUsage(float diskIOUsage){
            this.diskIOUsage = diskIOUsage;
        }
        public float getDiskIOUsage(){
            return diskIOUsage;
        }



        /**
         * Will return LONG_MAX if space hasn't been measured yet.
         * @return bytes of available local disk space on this tasktracker.
         */    
        long getAvailableSpace() {
            return availableSpace;
        }

        public void write(DataOutput out) throws IOException {
            WritableUtils.writeVLong(out, totalVirtualMemory);
            WritableUtils.writeVLong(out, totalPhysicalMemory);
            WritableUtils.writeVLong(out, mapSlotMemorySizeOnTT);
            WritableUtils.writeVLong(out, reduceSlotMemorySizeOnTT);
            WritableUtils.writeVLong(out, availableSpace);
            
            // Piggyback heartbeat
            WritableUtils.writeVLong(out, availableVirtualMemory);
            WritableUtils.writeVLong(out, availablePhysicalMemory);
            WritableUtils.writeVLong(out, cumulativeCpuTime);
            WritableUtils.writeVLong(out, cpuFrequency);
            WritableUtils.writeVInt(out, numProcessors);
            out.writeFloat(cpuUsage);
            out.writeFloat(diskIOUsage);
            
            // private long totalVirtualMemory ;
            // private long totalPhysicalMemory;
            // private long mapSlotMemorySizeOnTT = UNAVAILABLE;
            // private long reduceSlotMemorySizeOnTT = UNAVAILABLE;
            // private long availableSpace ;
            // 
            // private long availableVirtualMemory = UNAVAILABLE;
            // private long availablePhysicalMemory = UNAVAILABLE;
            // private long cumulativeCpuTime = UNAVAILABLE;
            // private long cpuFrequency = UNAVAILABLE;
            // private int numProcessors = UNAVAILABLE;
            // private float cpuUsage = UNAVAILABLE;
            // private float diskIOUsage = UNAVAILABLE;
        }

        public void readFields(DataInput in) throws IOException {
            totalVirtualMemory = WritableUtils.readVLong(in);
            totalPhysicalMemory = WritableUtils.readVLong(in);
            mapSlotMemorySizeOnTT = WritableUtils.readVLong(in);
            reduceSlotMemorySizeOnTT = WritableUtils.readVLong(in);
            availableSpace = WritableUtils.readVLong(in);
            
            // Piggyback heartbeat
            availableVirtualMemory = WritableUtils.readVLong(in);
            availablePhysicalMemory = WritableUtils.readVLong(in);
            cumulativeCpuTime = WritableUtils.readVLong(in);
            cpuFrequency = WritableUtils.readVLong(in);
            numProcessors = WritableUtils.readVInt(in);
            setCpuUsage(in.readFloat());
            setDiskIOUsage(in.readFloat());
        }

}

    private ResourceStatus resStatus;

    /**
    */
    public TaskTrackerStatus() {
        taskReports = new ArrayList<TaskStatus>();
        resStatus = new ResourceStatus();
    }

    /**
    */
    public TaskTrackerStatus(String trackerName, String host, 
            int httpPort, List<TaskStatus> taskReports, 
            int failures, int maxMapTasks,
            int maxReduceTasks) {
        this.trackerName = trackerName;
        this.host = host;
        this.httpPort = httpPort;

        this.taskReports = new ArrayList<TaskStatus>(taskReports);
        this.failures = failures;
        this.maxMapTasks = maxMapTasks;
        this.maxReduceTasks = maxReduceTasks;
        this.resStatus = new ResourceStatus();
    }

    /**
    */
    public String getTrackerName() {
        return trackerName;
    }
    /**
    */
    public String getHost() {
        return host;
    }

    /**
     * Get the port that this task tracker is serving http requests on.
     * @return the http port
     */
    public int getHttpPort() {
        return httpPort;
    }

    /**
     * Get the number of tasks that have failed on this tracker.
     * @return The number of failed tasks
     */
    public int getFailures() {
        return failures;
    }

    /**
     * Get the current tasks at the TaskTracker.
     * Tasks are tracked by a {@link TaskStatus} object.
     * 
     * @return a list of {@link TaskStatus} representing 
     *         the current tasks at the TaskTracker.
     */
    public List<TaskStatus> getTaskReports() {
        return taskReports;
    }

    /**
     * Return the current MapTask count
     */
    public int countMapTasks() {
        int mapCount = 0;
        for (Iterator<TaskStatus> it = taskReports.iterator(); it.hasNext();) {
            TaskStatus ts = it.next();
            TaskStatus.State state = ts.getRunState();
            if (ts.getIsMap() &&
                    ((state == TaskStatus.State.RUNNING) ||
                     (state == TaskStatus.State.UNASSIGNED) ||
                     ts.inTaskCleanupPhase())) {
                mapCount++;
                     }
        }
        return mapCount;
    }

    /**
     * Return the current ReduceTask count
     */
    public int countReduceTasks() {
        int reduceCount = 0;
        for (Iterator<TaskStatus> it = taskReports.iterator(); it.hasNext();) {
            TaskStatus ts = it.next();
            TaskStatus.State state = ts.getRunState();
            if ((!ts.getIsMap()) &&
                    ((state == TaskStatus.State.RUNNING) ||  
                     (state == TaskStatus.State.UNASSIGNED) ||
                     ts.inTaskCleanupPhase())) {
                reduceCount++;
                     }
        }
        return reduceCount;
    }

    /**
    */
    public long getLastSeen() {
        return lastSeen;
    }
    /**
    */
    public void setLastSeen(long lastSeen) {
        this.lastSeen = lastSeen;
    }

    /**
     * Get the maximum concurrent tasks for this node.  (This applies
     * per type of task - a node with maxTasks==1 will run up to 1 map
     * and 1 reduce concurrently).
     * @return maximum tasks this node supports
     */
    public int getMaxMapTasks() {
        return maxMapTasks;
    }
    public int getMaxReduceTasks() {
        return maxReduceTasks;
    }  

    /**
     * Return the {@link ResourceStatus} object configured with this
     * status.
     * 
     * @return the resource status
     */
    ResourceStatus getResourceStatus() {
        return resStatus;
    }

    ///////////////////////////////////////////
    // Writable
    ///////////////////////////////////////////
    public void write(DataOutput out) throws IOException {
        UTF8.writeString(out, trackerName);
        UTF8.writeString(out, host);
        out.writeInt(httpPort);
        out.writeInt(failures);
        out.writeInt(maxMapTasks);
        out.writeInt(maxReduceTasks);
        resStatus.write(out);
        out.writeInt(taskReports.size());

        for (TaskStatus taskStatus : taskReports) {
            TaskStatus.writeTaskStatus(out, taskStatus);
        }
    }

    public void readFields(DataInput in) throws IOException {
        this.trackerName = UTF8.readString(in);
        this.host = UTF8.readString(in);
        this.httpPort = in.readInt();
        this.failures = in.readInt();
        this.maxMapTasks = in.readInt();
        this.maxReduceTasks = in.readInt();
        resStatus.readFields(in);
        taskReports.clear();
        int numTasks = in.readInt();

        for (int i = 0; i < numTasks; i++) {
            taskReports.add(TaskStatus.readTaskStatus(in));
        }
    }
}
