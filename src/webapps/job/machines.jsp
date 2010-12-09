<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="java.text.DecimalFormat"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
%>
<%!	private static final long serialVersionUID = 1L;
%>
<%
  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  String type = request.getParameter("type");
%>
<%!
  public void generateTaskTrackerTable(JspWriter out,
                                       String type,
                                       JobTracker tracker) throws IOException {
    Collection c;
    if (("blacklisted").equals(type)) {
      c = tracker.blacklistedTaskTrackers();
    } else if (("active").equals(type)) {
      c = tracker.activeTaskTrackers();
    } else {
      c = tracker.taskTrackers();
    }
    if (c.size() == 0) {
      out.print("There are currently no known " + type + " Task Trackers.");
    } else {
      out.print("<center>\n");
      out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n");
      out.print("<tr><td align=\"center\" colspan=\"6\"><b>Task Trackers</b></td></tr>\n");
      out.print("<tr><td><b>Name</b></td><td><b>Host</b></td>" +
                "<td><b># running tasks</b></td>" +
                "<td><b>Max Map Tasks</b></td>" +
                "<td><b>Max Reduce Tasks</b></td>" +
                "<td><b>Failures</b></td>" +
                "<td><b>Seconds since heartbeat</b></td>" +
                "<td><b>Avail Space</b></td></tr>\n");
      int maxFailures = 0;
      String failureKing = null;
      for (Iterator it = c.iterator(); it.hasNext(); ) {
        TaskTrackerStatus tt = (TaskTrackerStatus) it.next();
        long sinceHeartbeat = System.currentTimeMillis() - tt.getLastSeen();
        if (sinceHeartbeat > 0) {
          sinceHeartbeat = sinceHeartbeat / 1000;
        }
        int numCurTasks = 0;
        for (Iterator it2 = tt.getTaskReports().iterator(); it2.hasNext(); ) {
          it2.next();
          numCurTasks++;
        }
        int numFailures = tt.getFailures();
        if (numFailures > maxFailures) {
          maxFailures = numFailures;
          failureKing = tt.getTrackerName();
        }
        out.print("<tr><td><a href=\"http://");
        out.print(tt.getHost() + ":" + tt.getHttpPort() + "/\">");
        out.print(tt.getTrackerName() + "</a></td><td>");
        out.print(tt.getHost() + "</td><td>" + numCurTasks +
                  "</td><td>" + tt.getMaxMapTasks() +
                  "</td><td>" + tt.getMaxReduceTasks() + 
                  "</td><td>" + numFailures + 
                  "</td><td>" + sinceHeartbeat + "</td><td>" + tt.getResourceStatus().getAvailableSpace() + "</td></tr>\n");
      }
      out.print("</table>\n");
      out.print("</center>\n");
      if (maxFailures > 0) {
        out.print("Highest Failures: " + failureKing + " with " + maxFailures + 
                  " failures<br>\n");
      }
    }
  }


    public void generateTaskTrackerResourceTable(JspWriter out,
                                                 String type,
                                                 JobTracker tracker) 
    throws IOException {
        Collection c;
        if (("blacklisted").equals(type)) {
          c = tracker.blacklistedTaskTrackers();
        } else if (("active").equals(type)) {
          c = tracker.activeTaskTrackers();
        } else {
          c = tracker.taskTrackers();
        }
        if (c.size() == 0) {
          out.print("There are currently no known " + type + " Task Trackers.");
        } else {
          out.print("<center>\n");
          out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n");
          out.print("<tr><td align=\"center\" colspan=\"6\"><b>Task Trackers</b></td></tr>\n");
          out.print("<tr><td><b>Name</b></td><td><b>Host</b></td>" +
                    "<td><b>Running Tasks</b></td>" +
                    "<td><b>CPU Usage</b></td>" +
                    "<td><b>Disk I/O</b></td>" +
                    "<td><b>Memory Usage</b></td>\n");
          // Name | Host | Running Tasks | CPU Usage | Disk I/O | Memory Usage
          for (Iterator it = c.iterator(); it.hasNext(); ) {
            TaskTrackerStatus tt = (TaskTrackerStatus) it.next();
            StringBuilder tasks = new StringBuilder();
            for (Iterator it2 = tt.getTaskReports().iterator(); it2.hasNext(); ) {
                TaskStatus taskStat = (TaskStatus)it2.next();
                if (tasks.length() > 0) {
                    tasks.append(", ");
                }
                tasks.append(taskStat.getTaskID());
            }
            if (tasks.length() == 0) {
                tasks.append("No tasks running");
            }
            double freePhysicalMemoryPercent = tt.getResourceStatus().getAvailablePhysicalMemory() * 1.0 / 
                tt.getResourceStatus().getTotalPhysicalMemory();
            double physicalMemoryUsage = 1.0 - freePhysicalMemoryPercent;
            // Should add formatter
            out.print("<tr><td><a href=\"http://");
            out.print(tt.getHost() + ":" + tt.getHttpPort() + "/\">");
            out.print(tt.getTrackerName() + "</a></td><td>");
            out.print(tt.getHost() + "</td><td>" + tasks.toString() +
                      "</td><td>" + tt.getResourceStatus().getCpuUsage() +
                      "</td><td>" + tt.getResourceStatus().getDiskIOUsage() + 
                      "</td><td>" + physicalMemoryUsage + 
                      "</td></tr>\n");
          }
          out.print("</table>\n");
          out.print("</center>\n");
        }
      }
%>

<html>

<title><%=trackerName%> Hadoop Machine List</title>

<body>
<h1><a href="jobtracker.jsp"><%=trackerName%></a> Hadoop Machine List</h1>

<h2>Task Trackers</h2>
<%
  generateTaskTrackerTable(out, type, tracker);
%>
<hr>

<h2>Resource Utilization</h2>
<%
  generateTaskTrackerResourceTable(out, type, tracker);
%>

<%
out.println(ServletUtil.htmlFooter());
%>

<script type="text/javascript" charset="utf-8">
    // Auto reload
    window.onload = function() {
        setTimeout(function() { window.location.reload(); }, 2000);
    }
</script>