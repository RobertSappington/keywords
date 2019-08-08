# -*- coding: utf-8 -*-
import win_unicode_console, pandas, re, textwrap, pytz, datetime

# Enable utf-8 character display for Windows console output. Use only in debug mode.
win_unicode_console.enable()

max_length = 80
max_words = 10
batch_size = 1000
time_zone = pytz.timezone('US/Pacific')

def filter_data(black_list, raw_list):
    """ Remove items on blacklist. """

    unique_blocked = black_list.unique()
    unique_raw = raw_list.unique()
    # Check blacklist and remove blocked items.
    return (x for x in unique_raw if x not in unique_blocked)

def clean_list(filtered_list):
    """ Conform keywords to Amazon Ad requirements."""

    pruned_data = []
    for record in filtered_list:
        str_datum = str(record)
        # Replace characters outside ASCII codec and remove invalid characters.
        clean_record = str_datum.translate(str_datum.maketrans('-_–―`åÁéäñćëèóōø—áÑãśŽØæěšńÃòúößçğÅčíž','     aAeanceeooo aNasZOaesnAouobcgAciz','.,<>(){}[]\'\"#@^!*?:/\\&’™;~$♂|®+…´+¡®©%=′£“”·‘¿'))
        # Truncate long strings to nearest whole word less than limit.
        cut_record = textwrap.shorten(clean_record, max_length, placeholder='')
        # Reduce word count to keyword max.
        count = len(re.findall(r'\w+', cut_record))
        shrink = max_length
        if count > max_words:
            while count > max_words:
                shrink -= 1
                temp = textwrap.shorten(cut_record, shrink, placeholder='')
                count = len(re.findall(r'\w+', temp))
                if count <= max_words:
                    pruned_data.append(temp)
        else:
            pruned_data.append(cut_record)
    # Remove duplicates.
    unique = set(pruned_data)
    return unique

def write_data(path, clean_data, batch_files, genre, type):
    """Save data in unique batches and incremental sets with file name series stamps."""

    part_filled = False
    partial_file = ''
    partial_batch = []
    stamp = str(datetime.datetime.now(tz = time_zone)).replace(':', '-')
    batch = set()
    all_batches = set()
    data_series = set()
    # Load aggregate data.
    data_series = set(clean_data)
    data_series.discard('\n')
    series_count = 0
    # Load saved batches.
    with open(path + batch_files, 'r+', encoding='utf-8') as file_list:
        batch_list = [line.rstrip('\n') for line in file_list]
        batch_names = list(filter(None, batch_list))
        series_count = len(batch_names)
        # Initialize an empty batch if none exists.
        if series_count == 0:    # Python phrasing: "if not batch_names:"
            series_count += 1
            partial_file = genre + type + '_data_' + str(series_count) + '_' + stamp + '.txt'
            with open(path + partial_file, 'w+', encoding='utf-8'):
                pass
            file_list.writelines('%s\n' % partial_file)
            batch_names.append(partial_file)
        # Load existing batches.
        for file_name in batch_names:
            with open(path + file_name, 'r', encoding='utf-8') as tranche:
                batch = set(line.rstrip('\n') for line in tranche)
                all_batches |= batch
                # Check if last batch is partial.
                if file_name == batch_names[-1]:
                    if len(batch) < batch_size:
                        part_filled = True
                        partial_batch = list(batch)
                        partial_file = file_name
    # Find incremental data.
    inc_data = list(data_series - all_batches)
    # Save incremental data.
    if len(inc_data) > 0:
        inc_file = 'inc_' + genre + type + '_data_' + stamp + '.txt'
        with open(path + inc_file, 'w', encoding='utf-8') as delta:
            delta.writelines('%s\n' % item for item in inc_data)
    # Save data batches.
    while len(inc_data) > 0:
        # Fill the incomplete batch.
        if part_filled == True:
            partial_space = batch_size - len(partial_batch)
            fill_batch = partial_batch + inc_data[:partial_space]
            with open(path + partial_file, 'w', encoding='utf-8') as full_batch:
                full_batch.writelines('%s\n' % item for item in fill_batch)
            inc_data[:partial_space] = []
            part_filled = False
        # Save remaining data to a new batch.
        if len(inc_data) > 0:
            series_count += 1
            fill_file = genre + type + '_data_' + str(series_count) + '_' + stamp + '.txt'
            with open(path + fill_file, 'w', encoding='utf-8') as partial:
                partial.writelines('%s\n' % item for item in inc_data[:batch_size])
            with open(path + batch_files, 'a+', encoding='utf-8') as file_list:
                file_list.writelines('%s\n' % fill_file)
            inc_data[:batch_size] = []

def main():
    # Set range to n+1 to process all genres.
    for i in range(2):
        select_genre = i
        if select_genre == 0:
            path = 'C:/'
            data_file = 'data0.txt'
            author_batches = 'AuthorBatches0.txt'
            title_batches = 'TitleBatches0.txt'
            genre = '0'
        elif select_genre == 1:
            path = 'C:/'
            data_file = 'data1.txt'
            author_batches = 'AuthorBatches1.txt'
            title_batches = 'TitleBatches1.txt'
            genre = '1'
        block_file = genre + 'Blacklist.txt'
    
        with open(path + data_file,'r', encoding='utf-8') as raw_file, open(path + block_file, 'r', encoding='utf-8') as blocked_file:
            try:
                df = pandas.read_csv(raw_file, sep='\t', header=None)
                df_blocked = pandas.read_csv(blocked_file, sep='\t', header=None)
                authors_raw = df.iloc[:, 1]
                blocked_authors = df_blocked.iloc[:, 1]
                authors_filtered = filter_data(blocked_authors, authors_raw)
                authors_deduped = list(filter(None, clean_list(authors_filtered)))
                write_data(path, authors_deduped, author_batches, genre, 'Author')
                titles_raw = df.iloc[:, 2]
                blocked_titles = df_blocked.iloc[:, 2]
                titles_filtered = filter_data(blocked_titles, titles_raw)
                titles_deduped = list(filter(None, clean_list(titles_filtered)))
                write_data(path, titles_deduped, title_batches, genre, 'Title')
            except pandas.errors.EmptyDataError:
                pass

if __name__ == "__main__":
    main()
