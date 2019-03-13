import logging

from utilities.MiscUtils import MiscUtils

logger = logging.getLogger('gottrain_logger')


class JobStatus:

    def __init__(self, layer, table_name, conn):
        self.start_time_epoch = MiscUtils.get_epoch()
        self.start_time = MiscUtils.get_timestamp()
        self.end_time_epoch = None
        self.end_time = None
        self.layer = layer
        self.table_name = table_name
        self.source = None
        self.inserts = None
        self.updates = None
        self.deletes = None
        self.status = 'STARTED'
        self.conn = conn
        self.job_id = self.conn.get_next_job_id()
        self.start_job()

    def start_job(self):
        logger.info("******************** Starting %s landing job at %s UTC ********************",
                    self.table_name, self.start_time)
        self.conn.start_job_status(self.job_id, self.layer, self.table_name, self.start_time)

    def end_job(self, status='FINISHED'):
        self.end_time_epoch = MiscUtils.get_epoch()
        self.end_time = MiscUtils.get_timestamp()
        self.conn.update_job_status(self.end_time, self.job_id, status)
        logger.info("******************** Finished %s landing job at %s UTC ********************",
                    self.table_name, self.end_time)
        logger.info("******************** Total time elapsed for %s staging job: %s seconds ********************",
                    self.table_name, self.end_time_epoch - self.start_time_epoch + 1)

    def insert_stats(self, source, inserts, updates, deletes):
        self.source = source
        self.inserts = inserts
        self.updates = updates
        self.deletes = deletes

        logger.info("******************** Updates: %d, Inserts %d, Deletes: %d ********************",
                    updates, inserts, deletes)
        self.conn.insert_job_counts(self.job_id, self.layer, self.table_name, self.start_time, self.end_time,
                                    self.source, self.inserts, self.updates, self.deletes)
