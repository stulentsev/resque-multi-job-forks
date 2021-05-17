require 'resque'
require 'resque/worker'
require 'resque/plugins/multi_job_forks/rss_reader'

module Resque
  class Worker
    include Plugins::MultiJobForks::RssReader
    attr_accessor :seconds_per_fork
    attr_accessor :jobs_per_fork
    attr_accessor :memory_threshold
    attr_reader   :jobs_processed

    WorkerTerminated = Class.new(StandardError)

    def self.multi_jobs_per_fork?
      ENV["DISABLE_MULTI_JOBS_PER_FORK"].nil?
    end

    if multi_jobs_per_fork? && !method_defined?(:shutdown_without_multi_job_forks)

      def fork(&block)
        if child = Kernel.fork
          return child
        else
          if term_child
            unregister_signal_handlers
            trap('TERM') do
              trap('TERM') do
                # Ignore subsequent term signals
              end

              if @performing_job
                # If a job is in progress, stop it immediately.
                raise TermException.new("SIGTERM")
              else
                # If we're not currently running a job, shut down cleanly.
                # This allows us to push unworked jobs back on the queue.
                shutdown
              end
            end
            trap('QUIT') { shutdown }
          end
          raise NotImplementedError, "Pretending to not have forked"
          # perform_with_fork will run the job and continue working...
        end
      end

      def work_with_multi_job_forks(*args)
        pid # forces @pid to be set in the parent
        work_without_multi_job_forks(*args)
        release_and_exit! unless is_parent_process?
      end
      alias_method :work_without_multi_job_forks, :work
      alias_method :work, :work_with_multi_job_forks

      def perform_with_multi_job_forks(job = nil)
        @fork_per_job = true unless fork_hijacked? # reconnect and after_fork
        if shutdown?
          # We got a request to shut down _after_ grabbing a job but _before_ starting work
          # on it. Immediately report the job as failed and return.
          if job
            report_failed_job(job, WorkerTerminated.new("shutdown before job start"))
          end
          return
        end
        @performing_job = true
        perform_without_multi_job_forks(job)
        hijack_fork unless fork_hijacked?
        @jobs_processed += 1
      ensure
        @performing_job = false
      end
      alias_method :perform_without_multi_job_forks, :perform
      alias_method :perform, :perform_with_multi_job_forks

      def shutdown_with_multi_job_forks?
        release_fork if fork_hijacked? && (fork_job_limit_reached? || @shutdown)
        shutdown_without_multi_job_forks?
      end
      alias_method :shutdown_without_multi_job_forks?, :shutdown?
      alias_method :shutdown?, :shutdown_with_multi_job_forks?

      def shutdown_with_multi_job_forks
        shutdown_child
        shutdown_without_multi_job_forks
      end
      alias_method :shutdown_without_multi_job_forks, :shutdown
      alias_method :shutdown, :shutdown_with_multi_job_forks

      def pause_processing_with_multi_job_forks
        shutdown_child
        pause_processing_without_multi_job_forks
      end
      alias_method :pause_processing_without_multi_job_forks, :pause_processing
      alias_method :pause_processing, :pause_processing_with_multi_job_forks

      def working_on_with_worker_registration(job)
        register_worker
        working_on_without_worker_registration(job)
      end
      alias_method :working_on_without_worker_registration, :working_on
      alias_method :working_on, :working_on_with_worker_registration

      # Reconnect only once
      def reconnect_with_multi_job_forks
        unless @reconnected
          reconnect_without_multi_job_forks
          @reconnected = true
        end
      end
      alias_method :reconnect_without_multi_job_forks, :reconnect
      alias_method :reconnect, :reconnect_with_multi_job_forks
    end

    # Need to tell the child to shutdown since it might be looping performing
    # multiple jobs per fork. The QUIT signal normally does a graceful shutdown,
    # and is re-registered in children (term_child normally unregisters it).
    def shutdown_child
      return unless @child
      begin
        log_with_severity :debug, "multi_jobs_per_fork: Sending QUIT signal to #{@child}"
        Process.kill('QUIT', @child)
      rescue Errno::ESRCH
        nil
      end
    end

    def is_parent_process?
      @child || @pid == Process.pid
    end

    def release_and_exit!
      release_fork if fork_hijacked?
      run_at_exit_hooks ? exit : exit!(true)
    end

    def fork_hijacked?
      @release_fork_limit
    end

    def hijack_fork
      log_with_severity :debug, 'hijack fork.'
      @suppressed_fork_hooks = [Resque.after_fork, Resque.before_fork]
      Resque.after_fork = Resque.before_fork = nil
      @release_fork_limit = fork_job_limit
      @jobs_processed = 0
      @fork_per_job = false
    end

    def release_fork
      log_with_severity :info, "jobs processed by child: #{jobs_processed}; rss: #{rss}"
      run_hook :before_child_exit, self
      Resque.after_fork, Resque.before_fork = *@suppressed_fork_hooks
      @release_fork_limit = @jobs_processed = nil
      log_with_severity :debug, 'hijack over, counter terrorists win.'
      @shutdown = true
    end

    def fork_job_limit
      jobs_per_fork.nil? ? Time.now.to_f + seconds_per_fork : jobs_per_fork
    end

    def fork_job_limit_reached?
      fork_job_limit_remaining <= 0 || fork_job_over_memory_threshold?
    end

    def fork_job_limit_remaining
      jobs_per_fork.nil? ? @release_fork_limit - Time.now.to_f : jobs_per_fork - @jobs_processed
    end

    def seconds_per_fork
      @seconds_per_fork ||= minutes_per_fork * 60
    end

    def minutes_per_fork
      ENV['MINUTES_PER_FORK'].nil? ? 1 : ENV['MINUTES_PER_FORK'].to_i
    end

    def jobs_per_fork
      @jobs_per_fork ||= ENV['JOBS_PER_FORK'].nil? ? nil : ENV['JOBS_PER_FORK'].to_i
    end

    def fork_job_over_memory_threshold?
      !!(memory_threshold && rss > memory_threshold)
    end

    def memory_threshold
      @memory_threshold ||= ENV["RESQUE_MEM_THRESHOLD"].to_i
      @memory_threshold > 0 && @memory_threshold
    end

  end

  # the `before_child_exit` hook will run in the child process
  # right before the child process terminates
  #
  # Call with a block to set the hook.
  # Call with no arguments to return the hook.
  def self.before_child_exit(&block)
    block ? register_hook(:before_child_exit, block) : hooks(:before_child_exit)
  end

  # Set the before_child_exit proc.
  def self.before_child_exit=(before_child_exit)
    register_hook(:before_child_exit, block)
  end
end
