#include "rpl_slave_snapshot_manager.h"
#include "debug_sync.h"

bool Snapshot_manager::init()
{
  DBUG_ASSERT(m_rli->mts_groups_assigned == 0);
  return update_snapshot(true);
}

bool Snapshot_manager::update_snapshot(bool force)
{
  const auto lwm_seqno= m_rli->gaq->lwm.total_seqno;

  bool ret= true;
  std::chrono::system_clock::time_point now;
  std::chrono::microseconds diff_ms;
  std::chrono::microseconds period_ms {opt_mts_checkpoint_period};
  double pro;
  ulonglong seqno;

  mysql_mutex_lock(&m_mutex);

  DBUG_ASSERT(force || lwm_seqno <= m_next_seqno);

  if (!force && lwm_seqno < m_next_seqno)
  {
    goto end;
  }

  DBUG_ASSERT(force || lwm_seqno == m_next_seqno);

  if (m_rli->info_thd->create_explicit_snapshot(false))
  {
    sql_print_error("Error while creating explicit snapshot!");
    ret= false;
    goto end;
  }

  m_snapshot= m_rli->info_thd->get_explicit_snapshot();

  now= std::chrono::system_clock::now();
  diff_ms= std::chrono::duration_cast<std::chrono::microseconds>(now - m_last_snapshot_ms);

  m_last_snapshot_ms= now;

  sql_print_information("jhelt,snap_ms,%llu,%lu", m_next_seqno, m_last_snapshot_ms.time_since_epoch().count());

  if (!force && (diff_ms > period_ms || (period_ms - diff_ms) < m_behind_ms)) { // Falling behind OR catching up
    m_behind_ms= m_behind_ms + diff_ms - period_ms;
  } else { // Fully caught up
    m_behind_ms= std::chrono::microseconds {0};
  }

  DBUG_ASSERT(force || m_rli->mts_groups_assigned >= m_next_seqno);

  pro= (double)period_ms.count()/(period_ms + m_behind_ms).count();
  seqno= (ulonglong)(pro * (m_rli->mts_groups_assigned - m_next_seqno));
  // Advance next snapshot sequence number
  if (m_next_seqno == m_rli->mts_groups_assigned || seqno == 0) {
    // Edge case when no txns are scheduled
    m_next_seqno= m_next_seqno + 1;
  } else {
    // Regular case
    m_next_seqno= m_next_seqno + seqno;
  }

  mysql_cond_broadcast(&m_cond);

end:
  mysql_mutex_unlock(&m_mutex);

  DBUG_EXECUTE_IF(
      "wait_for_repl_update_snapshot",
      {
        const char act[]= "now signal reached wait_for continue";
        DBUG_ASSERT(!debug_sync_set_action(current_thd, STRING_WITH_LEN(act)));
      };
  );

  return ret;
}
