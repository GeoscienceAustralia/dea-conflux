from click import Option, UsageError


# From https://gist.github.com/jacobtolar/fb80d5552a9a9dfc32b12a829fa21c0c
class MutuallyExclusiveOption(Option):
    def __init__(self, *args, **kwargs):
        self.mutually_exclusive = set(kwargs.pop('mutually_exclusive', []))
        help = kwargs.get('help', '')
        if self.mutually_exclusive:
            ex_str = ', '.join(self.mutually_exclusive)
            kwargs['help'] = help + (
                ' NOTE: This argument is mutually exclusive with '
                ' arguments: [' + ex_str + '].'
            )
        super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        if self.mutually_exclusive.intersection(opts) and self.name in opts:
            raise UsageError(
                "Illegal usage: `{}` is mutually exclusive with "
                "arguments `{}`.".format(
                    self.name,
                    ', '.join(self.mutually_exclusive)
                )
            )

        return super(MutuallyExclusiveOption, self).handle_parse_result(
            ctx,
            opts,
            args
        )


def command_required_option_from_option(require_name, require_map):

    class CommandOptionRequiredClass(click.Command):

        def invoke(self, ctx):
            require = ctx.params[require_name]
            if require not in require_map:
                raise click.ClickException(
                    "Unexpected value for --'{}': {}".format(
                        require_name, require))
            if ctx.params[require_map[require].lower()] is None:
                raise click.ClickException(
                    "With {}={} must specify option --{}".format(
                        require_name, require, require_map[require]))
            super(CommandOptionRequiredClass, self).invoke(ctx)

    return CommandOptionRequiredClass
