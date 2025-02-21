import asyncio
import logging
import traceback
from datetime import datetime, timedelta
import configparser
import aiohttp
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WriteOptions


# 配置日志
logging.basicConfig(
    filename='../log/crawl_grafana_by_name.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

GAP = "1min"
GAP_SECONDS = 60 * 1

# Create a ConfigParser object
config = configparser.ConfigParser()
config.read('../project.ini')

CONFIG = {
    "csv_path": "../data/addr_uid.csv",
    "influxdb": {
        "url": config.get("influxdb", "remote_url"),
        "token": config.get("influxdb", "remote_admin_token"),
        "org": config.get("influxdb", "remote_org"),
        "bucket": f"{config.get('influxdb', 'remote_bucket_prefix')}{GAP}",
        "batch_size": config.get("influxdb", "remote_batch_size"),
        "flush_interval": config.get("influxdb", "remote_flush_interval")
    },
    "concurrency": 256,
    "targets": [
        f"summarize(stats.gauges.hubble.*.*.*.*, '{GAP}', 'avg', false)",
        f"summarize(stats.gauges.hubble.*.*.*, '{GAP}', 'avg', false)",
        f"summarize(stats.gauges.hubble.*.*, '{GAP}', 'avg', false)",
        f"summarize(stats.gauges.hubble.*, '{GAP}', 'avg', false)",
        f"summarize(stats.hubble.*.*.*.*, '{GAP}', 'avg', false)",
        f"summarize(stats.hubble.*.*.*, '{GAP}', 'avg', false)",
        f"summarize(stats.hubble.*.*, '{GAP}', 'avg', false)",
        f"summarize(stats.hubble.*, '{GAP}', 'avg', false)",
        f"summarize(stats_counts.hubble.*.*.*.*.*, '{GAP}', 'avg', false)",
        f"summarize(stats_counts.hubble.*.*.*.*, '{GAP}', 'avg', false)",
        f"summarize(stats_counts.hubble.*.*.*, '{GAP}', 'avg', false)",
        f"summarize(stats_counts.hubble.*.*, '{GAP}', 'avg', false)",
        f"summarize(stats_counts.hubble.*, '{GAP}', 'avg', false)",
        f"summarize(stats.timers.hubble.*.*.*.*.*, '{GAP}', 'avg', false)",
        f"summarize(stats.timers.hubble.*.*.*.*, '{GAP}', 'avg', false)",
        f"summarize(stats.timers.hubble.*.*.*, '{GAP}', 'avg', false)",
        f"summarize(stats.timers.hubble.*.*, '{GAP}', 'avg', false)",
        f"summarize(stats.timers.hubble.*, '{GAP}', 'avg', false)",
    ],
    "tags": [
        "error_code",
        "fid",
        "kind",
        "message_type",
        "method",
        "name",
        "peer_id",
        "reason",
        "source",
        "store",
        "topic",
        "unexpected",
        "valid"
    ],
    "time_window_days": 5,
    "max_retries": 10,
    "retry_backoff_base": 2,
    "end_time": "2025-02-20 00:00:00",
    "start_time": "2025-02-01 00:00:00"
}


class EnhancedDataProcessorName:
    def __init__(self):
        # 新增失败队列和写入锁
        self._write_buffer = asyncio.Queue(maxsize=10 * CONFIG["influxdb"]["batch_size"])
        self._failed_queue = asyncio.Queue()  # 失败数据重试队列
        self._write_lock = asyncio.Lock()  # 写入操作锁
        self._last_flush = datetime.now()
        self._client = InfluxDBClient(
            url=CONFIG["influxdb"]["url"],
            token=CONFIG["influxdb"]["token"]
        )
        # 使用更可靠的写入配置
        self._write_api = self._client.write_api(
            write_options=WriteOptions(
                batch_size=CONFIG["influxdb"]["batch_size"],
                flush_interval=CONFIG["influxdb"]["flush_interval"],
                retry_interval=5_000,  # 5秒重试间隔
                max_retries=5,  # 客户端内置重试
                max_retry_delay=30_000  # 30秒最大重试延迟
            )
        )
        self._flush_task = asyncio.create_task(self._auto_flush())
        self._retry_task = asyncio.create_task(self._retry_failed_writes())

    async def _retry_failed_writes(self):
        """失败数据重试后台任务"""
        while True:
            try:
                failed_batch = await self._failed_queue.get()
                logger.warning(f"重试失败批次 ({len(failed_batch)} points)")
                await self._safe_write_with_retry(failed_batch)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"重试任务异常: {str(e)}")

    async def _safe_write_with_retry(self, points: list):
        """带队列恢复的写入"""
        attempt = 0
        max_retries = CONFIG["max_retries"]
        while attempt < max_retries:
            try:
                async with self._write_lock:
                    # 同步写入确保顺序
                    self._write_api.write(
                        bucket=CONFIG["influxdb"]["bucket"],
                        org=CONFIG["influxdb"]["org"],
                        record=points
                    )
                    # 确认写入成功
                    self._write_api.flush()
                    logger.info(f"成功写入确认 {len(points)} 数据点")
                    return True
            except Exception as e:
                attempt += 1
                logger.warning(f"写入失败 (尝试 {attempt}/{max_retries}): {str(e)}")
                if attempt >= max_retries:
                    await self._failed_queue.put(points)  # 超过重试次数入队
                    return False
                await asyncio.sleep(CONFIG["retry_backoff_base"] ** attempt)

    async def _auto_flush(self):
        """增强的自动刷新"""
        while True:
            try:
                # 优先处理失败队列
                if not self._failed_queue.empty():
                    failed_batch = await self._failed_queue.get()
                    await self._safe_write_with_retry(failed_batch)

                # 处理正常队列
                buffer_to_send = []
                while not self._write_buffer.empty() and len(buffer_to_send) < CONFIG["influxdb"]["batch_size"]:
                    point = await self._write_buffer.get()
                    if point:
                        buffer_to_send.append(point)

                if buffer_to_send:
                    logger.debug(f"正在写入 {len(buffer_to_send)} 数据点")
                    success = await self._safe_write_with_retry(buffer_to_send)
                    if not success:
                        logger.error(f"写入失败，保留 {len(buffer_to_send)} 数据点待重试")
                        await self._failed_queue.put(buffer_to_send)
                    else:
                        self._last_flush = datetime.now()

                await asyncio.sleep(CONFIG["influxdb"]["flush_interval"])

            except asyncio.CancelledError:
                logger.info("自动刷新任务停止")
                break
            except Exception as e:
                logger.error(f"自动刷新异常: {str(e)}")

    async def add_point(self, point: Point):
        """增强的点写入"""
        await self._write_buffer.put(point)
        # 实时刷新检查
        if (datetime.now() - self._last_flush).total_seconds() > CONFIG["influxdb"]["flush_interval"]:
            await self._auto_flush()

    async def close(self):
        """增强的关闭流程"""
        # 停止后台任务
        self._flush_task.cancel()
        self._retry_task.cancel()

        try:
            await asyncio.gather(self._flush_task, self._retry_task, return_exceptions=True)
        finally:
            # 最终写入尝试
            final_batches = []

            # 清空正常队列
            while not self._write_buffer.empty():
                final_batches.append(await self._write_buffer.get())

            # 清空失败队列
            while not self._failed_queue.empty():
                final_batches.append(await self._failed_queue.get())

            # 分批写入剩余数据
            for batch in final_batches:
                await self._safe_write_with_retry(batch)

            # 强制同步等待
            self._write_api.flush()
            self._client.close()
            logger.info("所有数据已持久化")

    async def load_progress(self, ip: str, target: str) -> datetime:
        """加载上次处理进度"""
        query = f'''
        from(bucket: "{CONFIG["influxdb"]["bucket"]}")
          |> range(start: -1000d)
          |> filter(fn: (r) => r["_measurement"] == "progress" and r.ip == "{ip}" and r.target == "{target}")
          |> first()
        '''
        try:
            tables = self._client.query_api().query(query, org=CONFIG["influxdb"]["org"])
            for table in tables:
                for record in table.records:
                    last_time = record.get_time()
                    last_time = last_time.replace(tzinfo=None)
                    # self._state.progress_cache[cache_key] = last_time
                    return last_time
        except Exception as e:
            logger.warning(f"加载进度失败: {str(e)}")
        # 默认返回10天前
        return datetime.strptime(CONFIG["end_time"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=None) - timedelta(days=CONFIG["time_window_days"])


def process_grafana_csv(file_path):
    """处理 CSV 文件"""
    df = pd.read_csv(file_path)
    df_filtered = df[df['uids'].notna()]
    df_filtered['post_urls'] = df_filtered.apply(
        lambda row: f"{row['urls']}/api/datasources/proxy/uid/{row['uids']}/render", axis=1)
    return df_filtered


def round_to_hour(dt):
    """Round the datetime to the nearest hour (remove minutes and seconds)."""
    return dt.replace(minute=0, second=0, microsecond=0)


async def fetch_data(session, url, target, time_range, retries=3):
    """获取数据"""
    params = {
        "target": target,
        "from": str(int(time_range["start"].timestamp())),
        "until": str(int(time_range["end"].timestamp())),
        "format": "json",
        "maxDataPoints": int((time_range["end"].timestamp() - time_range["start"].timestamp()) / GAP_SECONDS)
    }
    attempt = 0
    while attempt < retries:
        try:
            async with session.get(url, params=params, timeout=300) as response:
                if response.status == 404:
                    return None
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            attempt += 1
            logger.error(f"fetch_data异常: {url} - {params} - {str(e)}")
            if attempt >= retries:
                return None


def create_progress_point(ip, target, timestamp):
    """创建进度记录点"""
    return (
        Point("progress")
        .tag("ip", ip)
        .tag("target", target)
        .field("last_processed", timestamp.isoformat())
        .time(timestamp)
    )


async def process_time_range(processor, session, url, target, time_range):
    """增强的时间范围处理"""
    ip = url.split("//")[-1].split(":")[0]

    try:
        flag = False
        data = await fetch_data(session, url, target, time_range)
        if not data:
            return flag
        for target_data in data:
            name = target_data.get('tags', {}).get('name', None)
            if not name:
                continue
            for dp in target_data['datapoints']:
                if dp[0] is not None:
                    value = dp[0]
                    timestamp = dp[1]
                    point = (
                        Point(name)
                        .tag("ip", ip)
                        .field("value", float(value))
                        .time(datetime.utcfromtimestamp(timestamp))
                    )
                    await processor.add_point(point)
                    flag = True

        # 更新进度缓存
        return flag

    except Exception as e:
        logger.error(f"处理失败: {url}-{target}-{time_range}: {str(e)}")
        return False


async def worker(processor, session, semaphore, url, target):
    """增强的工作协程"""
    async with semaphore:
        ip = url.split("//")[-1].split(":")[0]

        # 加载上次处理进度
        # last_processed = await processor.load_progress(ip, target)
        # start_time = round_to_hour(last_processed)
        end_time = datetime.strptime(CONFIG["end_time"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=None)
        start_time =  end_time - timedelta(days=CONFIG["time_window_days"])
        while start_time < end_time:
            current_window_end = min(
                start_time + timedelta(days=CONFIG["time_window_days"]),
                end_time
            )
            success = await process_time_range(
                processor, session, url, target,
                {"start": start_time, "end": current_window_end}
            )
            progress_point = create_progress_point(ip, target, start_time)
            await processor.add_point(progress_point)
            if not success:
                break
            # start_time = current_window_end
            start_time = start_time - timedelta(days=CONFIG["time_window_days"])

async def main():
    processor = EnhancedDataProcessorName()
    # Prepare URL list
    df = process_grafana_csv(CONFIG["csv_path"])
    urls = df["post_urls"].unique()
    # Create semaphore for concurrency control
    semaphore = asyncio.Semaphore(CONFIG["concurrency"])
    async with aiohttp.ClientSession() as session:
        try:
            tasks = [
                worker(processor, session, semaphore, url, target)
                for url in urls
                for target in CONFIG["targets"]
            ]
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"An error occurred during processing: {e}")
            logger.error(traceback.format_exc())
        finally:
            await processor.close()  # Ensure processor is closed even if an error occurs


if __name__ == "__main__":
    # asyncio.run(main())
    print(datetime.now().second)