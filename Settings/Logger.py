import logging
from Settings.Manager import get_severity, args_parser, severity_from_str


args = args_parser.parse_args()
if args.sev is not None:
    severity = severity_from_str(args.sev)
    print(f"Severity set to {args.sev}")
else:
    severity = get_severity()

logger = logging.getLogger("PROXIMA")
logger.setLevel(severity)
fh = logging.StreamHandler()
fh.setFormatter(logging.Formatter("%(asctime)s[%(levelname)s] %(module)s.%(funcName)s: %(message)s",
                                  datefmt='%d-%b %H:%M:%S'))
logger.addHandler(fh)
