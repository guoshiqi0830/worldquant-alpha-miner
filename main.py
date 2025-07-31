from worldquant.service import WorldQuantService
from worldquant.alpha_template import AlphaTemplate
from loguru import logger
import sys
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_fields', '-d', action='store_true', help='refresh data_fields to local db')

    parser.add_argument('--refresh_alphas', '-r', action='store_true', help='refresh alphas to local db')
    parser.add_argument('--start_date', help='start date')
    parser.add_argument('--end_date', help='end date')
    parser.add_argument('--status', help='status')

    parser.add_argument('--simulation_status', '-P', action='store_true', help='print simulation status')
    parser.add_argument('--simulation_queue', '-q', help='load simulation_queue from template')
    parser.add_argument('--append', action='store_true', help='append simulation_queue')
    parser.add_argument('--shuffle', action='store_true', help='shuffle simulation_queue')
    parser.add_argument('--stats', action='store_true',  help='print stats')

    parser.add_argument('--check', '-c', nargs='?', const = True, help='refresh alpha checks for completed simulations')
    parser.add_argument('--simulation', '-s', nargs='?', const = True, help='start simulation')
    parser.add_argument('--submit', '-S', nargs='?', const = True, help='find and submit alpha, or specify the alpha_id')
    parser.add_argument('--debug', action='store_true', help='enable debug mode')
    
    parser.add_argument('--check_negative', '-n', nargs='?', const = True, help='check negative alpha')
    
    args = parser.parse_args()

    logger.remove()
    logger.add(
        sys.stdout,
        colorize=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>{message}</cyan>",
        level= "DEBUG" if args.debug else 'INFO'
    )
    
    service = WorldQuantService()

    if args.data_fields:
        service.refresh_datafields()
    
    if args.refresh_alphas:
        service.refresh_alphas(start_date = args.start_date, end_date=args.end_date, status = args.status)

    if args.simulation_status:
        service.print_simulation_status()
    
    if args.simulation_queue:
        at = AlphaTemplate(args.simulation_queue)
        at.load_simulation_queue(append = args.append)

    if args.simulation:
        kwargs = {}
        if args.simulation != True:
            kwargs["template_id"] = args.simulation
        kwargs["shuffle"] = args.shuffle
        kwargs["stats"] = args.stats
        service.simulate_from_alpha_queue(**kwargs)

    if args.check:
        if args.check == True:
            service.check_all_alphas()
        else:
            service.check_one_alpha(args.check)

    if args.submit:
        if args.submit == True:
            service.find_and_sumbit_alpha()
        else:
            service.submit_alpha(args.submit)

    if args.check_negative:
        service.check_negative_direction()
