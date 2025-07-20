from worldquant.service import WorldQuantService
from loguru import logger
import sys
import argparse


logger.remove()
logger.add(
    sys.stdout,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}",
    level="INFO"
)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_fields', '-d', action='store_true', help='refresh data_fields')
    parser.add_argument('--refresh_alphas', '-r', action='store_true', help='refresh alphas')
    parser.add_argument('--alpha_queue', '-q', help='refresh alpha_queue')
    parser.add_argument('--append', '-a', action='store_true', help='append alpha_queue')
    parser.add_argument('--check', '-c', nargs='?', const = True, help='refresh alpha checks for completed simulations')
    parser.add_argument('--simulation', '-s', nargs='?', const = True, help='start simulation')
    parser.add_argument('--submit', '-S', nargs='?', const = True, help='submit a single alpha')
    parser.add_argument('--parallelism', '-p', help='set up the parallelism of the job')
    
    args = parser.parse_args()
    
    service = WorldQuantService()

    if args.data_fields:
        service.refresh_datafields()
    
    if args.refresh_alphas:
        service.refresh_alphas()
    
    if args.alpha_queue:
        service.populate_alpha_queue(args.alpha_queue, args.append)
    
    parallelism =int(args.parallelism) if args.parallelism else None

    if args.simulation:
        kwargs = {}
        if args.simulation != True:
            kwargs["template_id"] = args.simulation
            if parallelism:
                kwargs["parallelism"] = parallelism
        service.simulate_from_alpha_queue(**kwargs)

    if args.check:
        if args.check == True:
            service.check_all_alpha()
        else:
            service.check_alpha(args.check)

    if args.submit:
        if args.submit == True:
            service.find_and_sumbit_alpha()
        else:
            service.submit_alpha(args.submit)
