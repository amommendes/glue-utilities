import logging


def validate_options_in_mode(options, mode, required_options, not_allowed_options):
    for option in required_options:
        if options.get(option) is None:
            raise AssertionError('Option %s is required for mode %s' % (option, mode))
    for option in not_allowed_options:
        if options.get(option) is not None:
            raise AssertionError('Option %s is not allowed for mode %s' % (option, mode))



def validate_aws_regions(region):
    """
    To validate the region in the input. The region list below may be outdated as AWS and Glue expands, so it only
    create an error message if validation fails.
    If the migration destination is in a region other than Glue supported regions, the job will fail.
    :return: None
    """
    if region is None:
        return
    aws_glue_regions = [
        'ap-northeast-1', # Tokyo
        'eu-west-1',# Ireland
        'us-east-1',# North Virginia
        'us-east-2',# Ohio
        'us-west-2',# Oregon
    ]
    aws_regions = aws_glue_regions + [
        'ap-northeast-2', # Seoul
        'ap-south-1', # Mumbai
        'ap-southeast-1', # Singapore
        'ap-southeast-2', # Sydney
        'ca-central-1', # Montreal
        'cn-north-1', # Beijing
        'cn-northwest-1', # Ningxia
        'eu-central-1', # Frankfurt
        'eu-west-2', # London
        'sa-east-1', # Sao Paulo
        'us-gov-west-1', # GovCloud
        'us-west-1' # Northern California
    ]

    error_msg = "Invalid region: {0}, the job will fail if the destination is not in a Glue supported region".format(region)
    if region not in aws_regions:
        logging.error(error_msg)
    elif region not in aws_glue_regions:
        logging.warn(error_msg)
