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

def unique_sorted_members(pairs):
    firsts = list(zip(*pairs))[0]
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

def prepare_to_run(df, pairs, commands):
    for pair in pairs:
        row = pair[0]
        col = pair[1]
        pos = position(df, row, col)
        if pos == "sut":
            template = commands["sut"]
        elif pos == "md":
            template = commands["md"]
        else:
            template = commands["slt"]
        args = yaml.dump([pair])
        command_yaml = subprocess.run(["curry-batch", template, "--dryrun"], input = args, stdout = subprocess.PIPE, text=True, check=True).stdout
        command = yaml.safe_load(command_yaml)

        df.at[row, col] = { "pair":pair, \
                            "pos": pos, \
                            "template": template, \
                            "command": command}        
    return df

def run(df):
    return df.map(lambda d: (d.update({"result": run_cmd(d["command"])}), d)[1])

@click.command()
@click.argument('command')
@click.option('--sut-command', default = None, help='Override command for strict upper triangle')
@click.option('--md-command', default = None, help='Override command for strict lower triangle')
@click.option('--slt-command', default = None, help='Override command for main diagonal')
@click.option('--dryrun', is_flag=True, default=False, help='Do not actually run the commands')
@click.option('--noyaml', is_flag=True, default=False, help='Echo the pandas DataFrame without dumping to YAML')
def versus(command, sut_command, md_command, slt_command, dryrun, noyaml):
    pairs = get_pairs()
    commands = get_commands(command, sut_command, md_command, slt_command)
    
    names = unique_sorted_members(pairs)
    df = pandas.DataFrame(columns=names, index=names)
    df = prepare_to_run(df, pairs, commands)
    if not dryrun:
        df = run(df)
    stdout = df if noyaml else yaml.dump(df.to_dict())
    click.echo(stdout)

if __name__ == '__main__':
    versus()