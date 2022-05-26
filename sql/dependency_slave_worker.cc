#include <my_global.h>
#include "debug_sync.h"
#include "dependency_slave_worker.h"
#include "log_event_wrapper.h"
#include "rpl_slave_commit_order_manager.h"
#include <time.h>
#include "rpl_slave_snapshot_manager.h"


bool append_item_to_jobs(slave_job_item *job_item,
                         Slave_worker *w,
                         Relay_log_info *rli);

void clear_current_group_events(Slave_worker *worker,
                                Relay_log_info *rli,
                                bool overfill);

Log_event_wrapper*
Dependency_slave_worker::get_begin_event(Commit_order_manager *co_mngr)
{
  Log_event_wrapper* ret= NULL;
  while (!info_thd->killed &&
         running_status == RUNNING &&
         (ret= c_rli->dequeue_dep()) == NULL)
  {
    my_sleep(1);
  }

  if (ret) {
    status_var_increment(info_thd->status_var.com_stat[SQLCOM_DEQUEUED]);
  }

  // case: place ourselves in the commit order queue
  if (ret && co_mngr != NULL)
  {
    DBUG_ASSERT(opt_mts_dependency_order_commits == DEP_ORDER_COMMITS_STRICT);
    set_current_db(ret->get_db());
    co_mngr->register_trx(this);
  }

  return ret;
}

// Pulls and executes events single group
// Returns true if the group executed successfully
bool Dependency_slave_worker::execute_group()
{
  /*
  while (true) {
    if (c_rli->queued_trx_count.load() > 600000)
    {
      break;
    }
  }
  */

  int err= 0;
  Commit_order_manager *commit_order_mngr= get_commit_order_manager();

  DBUG_ASSERT(current_event_index == 0);
  auto begin_event= get_begin_event(commit_order_mngr);
  auto ev= begin_event;

  while (ev)
  {
    if (unlikely(err= execute_event(ev)))
    {
      c_rli->dependency_worker_error= true;
      break;
    }

    // case: restart trx if temporary error, see @slave_worker_ends_group
    if (unlikely(trans_retries && current_event_index == 0))
    {
      ev= begin_event;
      continue;
    }
    finalize_event(ev);
    ev= ev->next();
  }

  // case: in case of error rollback if commit ordering is enabled
  if (unlikely(err && commit_order_mngr))
  {
    commit_order_mngr->report_rollback(this);
  }

  ulonglong prev_in_flight;
  if (likely(begin_event))
  {
    DBUG_ASSERT(c_rli->num_in_flight_trx > 0);
    prev_in_flight= c_rli->num_in_flight_trx.fetch_sub(1);
    if (prev_in_flight <= 2)
    {
      mysql_mutex_lock(&c_rli->dep_lock);
      mysql_cond_signal(&c_rli->dep_trx_all_done_cond);
      mysql_mutex_unlock(&c_rli->dep_lock);
    }
  }

  c_rli->executed_trx_count++;
  // c_rli->done_queue.push(begin_event);
  // c_rli->cleanup_group(begin_event);

  return err == 0 && !info_thd->killed && running_status == RUNNING;
}

int
Dependency_slave_worker::execute_event(Log_event_wrapper *ev)
{
  // wait for all dependencies to be satisfied
  if (unlikely(!ev->wait(this)))
  {
    return 1;
  }

  DBUG_EXECUTE_IF("dbug.dep_wait_before_update_execution",
    {
       if (ev->raw_event()->get_type_code() == UPDATE_ROWS_EVENT)
       {
         const char act[]= "now signal signal.reached wait_for signal.done";
         DBUG_ASSERT(opt_debug_sync_timeout > 0);
         DBUG_ASSERT(!debug_sync_set_action(info_thd, STRING_WITH_LEN(act)));
       }
     };);

  // case: there was an error in one of the workers, so let's skip execution of
  // events immediately
  if (unlikely(c_rli->dependency_worker_error))
    return 1;

  // case: the worker job queue is full, let's flush the queue to make progress
  if (unlikely(current_event_index >= jobs.size))
  {
    // Resets current_event_index to 0 and disables trx retires because we've
    // flushed the events
    mysql_mutex_lock(&jobs_lock);
    clear_current_group_events(this, c_rli, true);
    mysql_mutex_unlock(&jobs_lock);
  }

  // case: append to jobs queue only if this is not a trx retry, trx retries
  // resets @current_event_index, see @slave_worker_ends_group
  if (likely(current_event_index == jobs.len))
  {
    // NOTE: this is done so that @pop_jobs_item() can extract this event
    // although this is redundant it makes integration with existing code much
    // easier
    Slave_job_item item= { ev->raw_event() };
    if (append_item_to_jobs(&item, this, c_rli)) return 1;
    // ev->is_appended_to_queue= true;
  }
  // DBUG_ASSERT(ev->is_appended_to_queue);
  return ev->execute(this, this->info_thd, c_rli) == 0 ? 0 : -1;
}

void
Dependency_slave_worker::finalize_event(Log_event_wrapper *ev)
{
  /* Attempt to clean up entries from the key lookup
   *
   * There are two cases:
   * 1) The "value" of the key-value pair is equal to this event. In this case,
   *    remove the key-value pair from the map.
   * 2) The "value" of the key-value pair is _not_ equal to this event. In this
   *    case, leave it be; the event corresponds to a later transaction.
   */

  for (const auto& key : ev->keys)
  {
    Relay_log_info::Last_writer_map::accessor ac;    
    if (c_rli->dep_key_lookup.find(ac, key) && ac->second == ev)
    {
      c_rli->dep_key_lookup.erase(ac);
    }
  }
  ev->finalize();
}

Dependency_slave_worker::Dependency_slave_worker(Relay_log_info *rli
#ifdef HAVE_PSI_INTERFACE
                                   ,PSI_mutex_key *param_key_info_run_lock,
                                   PSI_mutex_key *param_key_info_data_lock,
                                   PSI_mutex_key *param_key_info_sleep_lock,
                                   PSI_mutex_key *param_key_info_thd_lock,
                                   PSI_mutex_key *param_key_info_data_cond,
                                   PSI_mutex_key *param_key_info_start_cond,
                                   PSI_mutex_key *param_key_info_stop_cond,
                                   PSI_mutex_key *param_key_info_sleep_cond
#endif
                                   ,uint param_id
    )
: Slave_worker(rli
#ifdef HAVE_PSI_INTERFACE
               ,param_key_info_run_lock, param_key_info_data_lock,
               param_key_info_sleep_lock, param_key_info_thd_lock,
               param_key_info_data_cond, param_key_info_start_cond,
               param_key_info_stop_cond, param_key_info_sleep_cond
#endif
               ,param_id
    )
{
}

void Dependency_slave_worker::start()
{
  DBUG_ASSERT(c_rli->dep_queue.empty() &&
              dep_key_lookup.empty() &&
              keys_accessed_by_group.empty() &&
              dbs_accessed_by_group.empty());

  while (execute_group());

  // case: cleanup if stopped abruptly
  if (running_status != STOP_ACCEPTED)
  {
    // tagging as exiting so Coordinator won't be able synchronize with it
    mysql_mutex_lock(&jobs_lock);
    running_status= ERROR_LEAVING;
    mysql_mutex_unlock(&jobs_lock);

    // Killing Coordinator to indicate eventual consistency error
    mysql_mutex_lock(&c_rli->info_thd->LOCK_thd_data);
    c_rli->info_thd->awake(THD::KILL_QUERY);
    mysql_mutex_unlock(&c_rli->info_thd->LOCK_thd_data);
  }
}

