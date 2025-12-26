import logging
import sys
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime

_LOGGER_NAME = "tiger_utils"
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
_LOG_DIR = os.path.join(_PROJECT_ROOT, 'logs')
os.makedirs(_LOG_DIR, exist_ok=True)
_LOG_BASENAME = f"tiger_utils_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
_LOG_FILE = os.path.join(_LOG_DIR, _LOG_BASENAME)
_MAX_LINES = 5000
_BACKUP_COUNT = 20

class LineRotatingFileHandler(RotatingFileHandler):
	"""
	Rotates log after a maximum number of lines, not bytes.
	"""
	def __init__(self, filename, maxLines, backupCount=0, encoding=None):
		super().__init__(filename, maxBytes=0, backupCount=backupCount, encoding=encoding)
		self.maxLines = maxLines
		self.lineCount = 0
		self._count_existing_lines()

	def _count_existing_lines(self):
		try:
			with open(self.baseFilename, 'r', encoding=self.encoding or 'utf-8') as f:
				self.lineCount = sum(1 for _ in f)
		except FileNotFoundError:
			self.lineCount = 0

	def emit(self, record):
		super().emit(record)
		self.lineCount += 1
		if self.lineCount >= self.maxLines:
			self.doRollover()
			self.lineCount = 0

def setup_logger():
	"""
	Set up a logger that logs to both stdout and a file at the project root.
	Log file is timestamped, rotates after 5000 lines, keeps last 20 logs.
	Call this once at program startup.
	"""
	logger = logging.getLogger(_LOGGER_NAME)
	logger.setLevel(logging.INFO)
	if not logger.handlers:
		# Console handler
		ch = logging.StreamHandler(sys.stdout)
		ch.setLevel(logging.INFO)
		ch_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
		ch.setFormatter(ch_formatter)
		logger.addHandler(ch)

		# Rotating file handler (by line count)
		fh = LineRotatingFileHandler(_LOG_FILE, maxLines=_MAX_LINES, backupCount=_BACKUP_COUNT, encoding="utf-8")
		fh.setLevel(logging.INFO)
		fh_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
		fh.setFormatter(fh_formatter)
		logger.addHandler(fh)
	return logger

def get_logger():
	"""
	Get the shared project logger for use in other modules.
	"""
	return logging.getLogger(_LOGGER_NAME)