#ifndef RPL_SLAVE_SNAPSHOT_MANAGER
#define RPL_SLAVE_SNAPSHOT_MANAGER

#include "my_global.h"
#include "rpl_rli.h"
#include "rpl_rli_pdb.h"

class Snapshot_manager
{
  Relay_log_info *m_rli= NULL;
  std::shared_ptr<explicit_snapshot> m_snapshot;

  mysql_mutex_t m_mutex;
  mysql_cond_t m_cond;
  ulonglong m_last_snapshot_ms= 0;
  ulonglong m_next_seqno= 0;
  ulong m_waiting= 0;
  uint m_behind_ms= 0;

public:
  Snapshot_manager(Relay_log_info *m_rli) : m_rli(m_rli)
  {
    mysql_mutex_init(0, &m_mutex, MY_MUTEX_INIT_FAST);
    mysql_cond_init(0, &m_cond, NULL);
  }

  bool init();
  bool update_snapshot(bool force= false);

  ulonglong get_last_snapshot_ms()
  {
    ulonglong t = -1;
    mysql_mutex_lock(&m_mutex);
    t = m_last_snapshot_ms;
    mysql_mutex_unlock(&m_mutex);
    return t;
  }

  uint get_behind_ms()
  {
    uint t = -1;
    mysql_mutex_lock(&m_mutex);
    t = m_behind_ms;
    mysql_mutex_unlock(&m_mutex);
    return t;
  }

  ulong get_waiting()
  {
    ulong w = -1;
    mysql_mutex_lock(&m_mutex);
    w = m_waiting;
    mysql_mutex_unlock(&m_mutex);
    return w;
  }

  void set_snapshot(THD *thd)
  {
    mysql_mutex_lock(&m_mutex);
    thd->set_explicit_snapshot(m_snapshot);
    mysql_mutex_unlock(&m_mutex);
  }

  std::shared_ptr<explicit_snapshot> get_snapshot()
  {
    mysql_mutex_lock(&m_mutex);
    auto ret= m_snapshot;
    mysql_mutex_unlock(&m_mutex);
    return ret;
  }

  void wait_for_snapshot(THD *thd, const ulonglong seqno)
  {
    DBUG_ASSERT(seqno <= m_rli->mts_groups_assigned);
    PSI_stage_info old_stage;

    mysql_mutex_lock(&m_mutex);
    m_waiting += 1;
    thd->ENTER_COND(&m_cond, &m_mutex,
                    &stage_worker_waiting_for_snapshot,
                    &old_stage);
    while (seqno > m_next_seqno)
    {
      mysql_cond_wait(&m_cond, &m_mutex);
    }
    m_waiting -= 1;
    thd->EXIT_COND(&old_stage);
  }

  bool move_next_seqno(const ulonglong seqno)
  {
    mysql_mutex_lock(&m_mutex);
    if (seqno <= m_next_seqno)
    {
      mysql_mutex_unlock(&m_mutex);
      return false;
    }
    m_next_seqno= seqno;
    mysql_cond_broadcast(&m_cond);
    mysql_mutex_unlock(&m_mutex);
    return true;
  }
};

#endif /*RPL_SLAVE_SNAPSHOT_MANAGER*/
