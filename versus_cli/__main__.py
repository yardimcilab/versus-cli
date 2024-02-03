import sys, yaml, click, subprocess, pandas

# Potential features
# Drop intermediates

# Set the maximum number of columns and rows to be displayed
pandas.set_option('display.max_columns', 10)
pandas.set_option('display.max_rows', 10)
pandas.set_option('display.max_colwidth', 500)

def get_pairs():
    stdin = sys.stdin.read()
    cmd = ["itertools-cli", "product", "2"]
    result = subprocess.run(cmd, input = stdin, stdout = subprocess.PIPE, text = True, check = True).stdout
    return yaml.safe_load(result)

def get_commands(command, sut_command, md_command, slt_command):
    sut = command if sut_command is None else sut_command
    md = command if md_command is None else md_command
    slt = command if slt_command is None else slt_command
    commands = {"sut":sut, "md": md, "slt":slt}
    return commands

def unique_sorted_members(filtered_input, caption_index):
    firsts = list(zip(*filtered_input))[caption_index]
    unique = list(set(firsts))
    return sorted(unique)

def position(df, row, col):
    # Get the numerical index of the row and column labels
    row_index = df.index.tolist().index(row)
    col_index = df.columns.tolist().index(col)
    
    # Determine the position
    if row_index < col_index:
        return "sut"
    elif row_index == col_index:
        return "md"
    else:
        return "slt"

def run_cmd(cmd):
    return subprocess.run(cmd, stdout = subprocess.PIPE, text=True, check=True, shell=True).stdout

def prepare_to_run(df, filtered_input, commands, caption_index):
    for input in filtered_input:
        row = input[caption_index[0]]
        col = input[caption_index[1]]
        pos = position(df, row, col)
        if pos == "sut":
            template = commands["sut"]
        elif pos == "md":
            template = commands["md"]
        else:
            template = commands["slt"]
        args = yaml.dump([input])
        command_yaml = subprocess.run(["curry-batch", template, "--dryrun"], input = args, stdout = subprocess.PIPE, text=True, check=True).stdout
        command = yaml.safe_load(command_yaml)

        df.at[row, col] = { "filtered_input":input, \
                            "pos": pos, \
                            "template": template, \
                            "command": command}        
    return df

def run(df):
    return df.map(lambda d: (d.update({"result": run_cmd(d["command"])}), d)[1])

def filter(input_filters, pairs):
    cmd = ["curry-batch", "echo '{1}'", "echo '{2}'"] + list(input_filters)
    filtered_input = subprocess.run(cmd, input=yaml.dump(pairs), stdout = subprocess.PIPE, text=True, check=True).stdout
    return yaml.safe_load(filtered_input)



@click.command()
@click.argument('input-filters', nargs=-1)
@click.argument('command', nargs=1)
@click.option('--sut-command', default = None, help='Override command for strict upper triangle')
@click.option('--md-command', default = None, help='Override command for strict lower triangle')
@click.option('--slt-command', default = None, help='Override command for main diagonal')
@click.option('--caption_index', nargs=2, default = [0, 1], help='Indices of input to use as row and column captions (index 0-1 are raw input strings; index 2+ are filters)')
@click.option('--dryrun', is_flag=True, default=False, help='Do not actually run the commands')
@click.option('--noyaml', is_flag=True, default=False, help='Echo the pandas DataFrame without dumping to YAML')
def versus(input_filters, command, sut_command, md_command, slt_command, caption_index, dryrun, noyaml):
    pairs = get_pairs()
    filtered_input = filter(input_filters, pairs)
    commands = get_commands(command, sut_command, md_command, slt_command)
    row_names = unique_sorted_members(filtered_input, caption_index[0])
    col_names = unique_sorted_members(filtered_input, caption_index[1])
    df = pandas.DataFrame(columns=row_names, index=row_names)
    df = prepare_to_run(df, filtered_input, commands, caption_index)
    if not dryrun:
        df = run(df)
    stdout = df if noyaml else yaml.dump(df.to_dict())
    click.echo(stdout)

if __name__ == '__main__':
    versus()