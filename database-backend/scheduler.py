import logging
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.executors.pool import ThreadPoolExecutor
from database import process_daily_user_distribution

# Configure logging for scheduler
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DailyDistributionScheduler:
    def __init__(self):
        """Initialize the scheduler with thread pool executor"""
        self.scheduler = AsyncIOScheduler(
            executors={
                'default': ThreadPoolExecutor(max_workers=1)
            },
            timezone='Asia/Almaty'  # Kazakhstan timezone
        )
        self.is_running = False
        
    async def start(self):
        """Start the scheduler"""
        try:
            if not self.is_running:
                # Schedule daily job at 9:00 AM Kazakhstan time
                self.scheduler.add_job(
                    func=self.run_daily_distribution,
                    trigger=CronTrigger(hour=9, minute=0),
                    id='daily_user_distribution',
                    name='Daily User Distribution Process',
                    replace_existing=True,
                    max_instances=1  # Prevent overlapping executions
                )
                
                self.scheduler.start()
                self.is_running = True
                logger.info("Daily distribution scheduler started successfully")
                logger.info("Next run scheduled for: 9:00 AM every day (Asia/Almaty timezone)")
                
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
            raise
    
    async def stop(self):
        """Stop the scheduler"""
        try:
            if self.is_running:
                self.scheduler.shutdown(wait=True)
                self.is_running = False
                logger.info("Daily distribution scheduler stopped")
        except Exception as e:
            logger.error(f"Error stopping scheduler: {e}")
    
    def run_daily_distribution(self):
        """Execute the daily user distribution process"""
        try:
            logger.info("=" * 60)
            logger.info("STARTING DAILY USER DISTRIBUTION PROCESS")
            logger.info(f"Execution time: {datetime.now().isoformat()}")
            logger.info("=" * 60)
            
            # Run the distribution process
            result = process_daily_user_distribution()
            
            # Log results
            if result["success"]:
                if result.get("skip_reason"):
                    logger.info(f"Process skipped: {result['skip_reason']}")
                    self._send_skip_notification(result)
                else:
                    logger.info(f"✅ Process completed successfully!")
                    logger.info(f"   - Campaigns found: {result['campaigns_found']}")
                    logger.info(f"   - Users found: {result['users_found']}")
                    logger.info(f"   - Users distributed: {result['users_distributed']}")
                    self._send_success_notification(result)
            else:
                logger.error(f"❌ Process failed: {result.get('error_message', 'Unknown error')}")
                logger.error(f"   - Stage: {result['process_stage']}")
                self._send_error_notification(result)
            
            logger.info("=" * 60)
            logger.info("DAILY USER DISTRIBUTION PROCESS COMPLETED")
            logger.info("=" * 60)
            
        except Exception as e:
            error_msg = f"Unexpected error in daily distribution: {str(e)}"
            logger.error(error_msg)
            self._send_critical_error_notification(error_msg)
    
    def _send_success_notification(self, result):
        """Send email notification for successful process"""
        try:
            from email_sender import send_daily_distribution_success_email
            send_daily_distribution_success_email(result)
        except Exception as e:
            logger.error(f"Failed to send success notification: {e}")
    
    def _send_skip_notification(self, result):
        """Send email notification when process is skipped"""
        try:
            from email_sender import send_daily_distribution_skip_email
            send_daily_distribution_skip_email(result)
        except Exception as e:
            logger.error(f"Failed to send skip notification: {e}")
    
    def _send_error_notification(self, result):
        """Send email notification for process errors"""
        try:
            from email_sender import send_daily_distribution_error_email
            send_daily_distribution_error_email(result)
        except Exception as e:
            logger.error(f"Failed to send error notification: {e}")
    
    def _send_critical_error_notification(self, error_msg):
        """Send email notification for critical errors"""
        try:
            from email_sender import send_daily_distribution_critical_error_email
            send_daily_distribution_critical_error_email(error_msg)
        except Exception as e:
            logger.error(f"Failed to send critical error notification: {e}")
    
    async def run_test_distribution(self):
        """Run a test distribution (for manual testing)"""
        try:
            logger.info("Running test daily distribution...")
            result = process_daily_user_distribution()
            logger.info(f"Test result: {result}")
            return result
        except Exception as e:
            logger.error(f"Test distribution failed: {e}")
            return {"success": False, "error": str(e)}
    
    def get_scheduler_status(self):
        """Get current scheduler status and next run time"""
        try:
            if not self.is_running:
                return {
                    "status": "stopped",
                    "next_run": None,
                    "jobs": []
                }
            
            jobs = []
            for job in self.scheduler.get_jobs():
                jobs.append({
                    "id": job.id,
                    "name": job.name,
                    "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                    "trigger": str(job.trigger)
                })
            
            return {
                "status": "running",
                "jobs": jobs,
                "timezone": str(self.scheduler.timezone)
            }
            
        except Exception as e:
            logger.error(f"Error getting scheduler status: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

# Global scheduler instance
daily_scheduler = DailyDistributionScheduler()

async def start_daily_scheduler():
    """Start the daily distribution scheduler"""
    await daily_scheduler.start()

async def stop_daily_scheduler():
    """Stop the daily distribution scheduler"""
    await daily_scheduler.stop()

def get_daily_scheduler_status():
    """Get the daily distribution scheduler status"""
    return daily_scheduler.get_scheduler_status()

async def test_daily_distribution():
    """Run a test of the daily distribution process"""
    return await daily_scheduler.run_test_distribution() 