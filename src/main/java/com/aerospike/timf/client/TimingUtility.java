package com.aerospike.timf.client;

public class TimingUtility {
    // Specify this as a reference to the class so if the name ever changes we get a compile error
    private static final String FILE_NAME = MonitoringAerospikeClient.class.getSimpleName() + ".java";
    private static final int STACK_TRACE_ELEMENTS_TO_PRINT = 7;

    private static ITimingNotifier notifier = null;
    private long startTime;
    private long submissionTime;
    private long resultsTime;
    private String description;
    private Object[] args;
    private String stackTrace;

    public TimingUtility start(String description, Object... args) {
        this.description = description;
        this.args = args;

        if (notifier != null && notifier.requiresStackTrace()) {
            this.stackTrace = getStackTrace();
        }
        this.startTime = System.nanoTime();
        return this;
    }

    private String getStackTrace() {
        StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        boolean found = false;
        StringBuffer sb = new StringBuffer(1000);
        int counter = 0;
        for (int i = 0; i < stackTraceElements.length; i++) {
            if (!found) {
                // Skip over the parts of the stack which are internal, up to and over the
                // MonitoringAerospikeClient
                if (FILE_NAME.equals(stackTraceElements[i].getFileName())) {
                    found = true;
                }
            } else {
                if (counter < STACK_TRACE_ELEMENTS_TO_PRINT) {
                    sb.append("  ").append(stackTraceElements[i].toString()).append('\n');
                    counter++;
                } else {
                    sb.append("  ... (").append(stackTraceElements.length - i).append(") more.");
                    break;
                }
            }
        }
        return sb.toString();
    }

    public void markSubmissionTime() {
        this.submissionTime = (System.nanoTime() - this.startTime);
    }

    public void markResultsTime() {
        this.resultsTime = (System.nanoTime() - this.startTime);
    }

    public void end() {
        long totalTime = (System.nanoTime() - this.startTime);
        if (TimingUtility.notifier != null) {
            TimingUtility.notifier.notify(totalTime / 1000, this.submissionTime / 1000, this.resultsTime / 1000,
                    description, stackTrace, args);
        }
    }

    public <T> T end(T result) {
        long totalTime = (System.nanoTime() - this.startTime);
        if (TimingUtility.notifier != null) {
            TimingUtility.notifier.notify(totalTime / 1000, this.submissionTime / 1000, this.resultsTime / 1000, result,
                    description, stackTrace, args);
        }
        return result;
    }

    public RuntimeException end(RuntimeException e) {
        long totalTime = (System.nanoTime() - this.startTime);
        if (TimingUtility.notifier != null) {
            TimingUtility.notifier.notify(totalTime / 1000, this.submissionTime / 1000, this.resultsTime / 1000, e,
                    description, stackTrace, args);
        }
        return e;
    }

    public static void setNotifier(ITimingNotifier notifier) {
        TimingUtility.notifier = notifier;
    }
}
