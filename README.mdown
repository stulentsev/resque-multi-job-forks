# resque-multi-job-forks

## Installation

Add this line to your application's Gemfile:

    gem 'resque-multi-job-forks'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install resque-multi-job-forks
    
## Usage

If you have very frequent and fast resque jobs, the overhead of forking and running your after_fork hook, might get too big. Using this resque plugin, you can have your workers perform more than one job, before terminating.

You simply specify the number of jobs you want each fork to process using the JOBS_PER_FORK environment variable:

    QUEUE=* JOBS_PER_FORK=1000 rake resque:work

This will have each fork process 1000 jobs, before terminating.

This plugin also defines a new hook, that gets called right before the fork terminates:

  Resque.before_child_exit do |worker|
    worker.log("#{worker.jobs_processed} were processed in this fork")
  end
  

## Note on Patches/Pull Requests
 
* Fork the project.
* Make your feature addition or bug fix.
* Add tests for it. This is important so I don't break it in a
  future version unintentionally.
* Commit, do not mess with rakefile, version, or history.
  (if you want to have your own version, that is fine but bump version in a commit by itself I can ignore when I pull)
* Send me a pull request. Bonus points for topic branches.

## Copyright

Copyright (c) 2010 Mick Staugaard. See LICENSE for details.
