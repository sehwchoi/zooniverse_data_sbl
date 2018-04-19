""" This script is a basic aggregation routine.  It serves as a framework that can be modified to
aggregate the flattened classification records from any project in a variety of ways, though it is
most useful for question and drawing tasks.  Transcriptions and survey tasks are better handled
with specialized scripts provided elsewhere in this repository."""

# depending on the functions and library members needed, additional modules may be required:
import csv
import sys
import json

csv.field_size_limit(sys.maxsize)

# set up the file locations (user specific) - There needs to be a input file and the aggregated
# out put file.  The input file is assumed to be sorted on the field we want to aggregate over.
location = r'/Users/stellachoi/Documents/SDL/zooniverse/SBL/' \
           r'sorted_flattened_south-bend-lead.csv'
outlocation = r'/Users/stellachoi/Documents/SDL/zooniverse/SBL/' \
           r'aggregate_south-bend-lead.csv'

field_1_name = 'testresult_exist'
field_2_name = "testresult_exist_yes_percent"
field_3_name = 'testresult_list'
field_4_name = 'testresult_classify'
field_5_name = 'testresult_classify_percent'
# The next section can contain any functions we wish to use to analyse, modify or manipulate the
# aggregated data.  Once all the data has been collected together for a particular selection criteria
# (usually by subject), we can perform any analysis that depends only on the aggregated data for that
# selection and is independent of data aggregated for any other selection.  For example we can find the
# average of some input over the classifications for some subject image, but we can not at this point
# compare that to the average of the same field for other subjects which have not yet been aggregated.
some_limit = -1

def process_aggregation(subj, cl_counter, aggregated_bin_1, aggregated_bin_3, aggregated_bin_4):
    # process the aggregated data for a subject.  The input variables are the function parameters and
    # the out_put can be any consistent function of those variables.  Typical processing should include
    # verifying enough (some_limit) valid classifications were aggregated (even though all subjects may be 
    # retired, there may be cases not all the classifications are valid. Other processing could include 
    # clustering drawing points, calculating vote fractions, applying a Bayesian pipeline, or simply
    # modifying the order or presentation format of the aggregate data.
    if cl_counter > some_limit: 
        out_put_1 = aggregated_bin_1
        out_put_2 = round(aggregated_bin_1[0] / cl_counter * 100, 2)
        out_put_3 = aggregated_bin_3
        if sum(aggregated_bin_4) > 0:
            out_put_4 = aggregated_bin_4
            out_put_4_sum = sum(aggregated_bin_4)
            out_put_5 = [round(value/out_put_4_sum * 100, 2) for value in aggregated_bin_4]
        else:
            out_put_4 = ''
            out_put_5 = ''

        # Once the aggregated data is processed and the out_put variables defined we can
        # set up and write the aggregated and processed data to a file.  The field names can be chosen to
        # make the data as useful as possible. They must match the fieldnames in the section below - both
        # in order and spelling.
        new_row = {'subject_ids': subj,
                   'classifications': cl_counter,
                   field_1_name: out_put_1,
                   field_2_name: out_put_2,
                   field_3_name: out_put_3,
                   field_4_name: out_put_4,
                   field_5_name: out_put_5
                   }
        writer.writerow(new_row)
        return True
    else:
        return False


# set up the output file names for the aggregated and processed data, and write the header.  The
# fieldnames and order must match the write block:
with open(outlocation, 'w', newline='') as file:
    fieldnames = ['subject_ids',
                  'classifications',
                  field_1_name,
                  field_2_name,
                  field_3_name,
                  field_4_name,
                  field_5_name]
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()

    # set up to read the flattened file
    with open(location) as f:
        r = csv.DictReader(f)

        # initialize a starting point subject and empty bins for aggregation
        subject = ''
        i = 1
        bin_1 = ['' for count in range(0, 2)]   # example for a list of known length
        bin_4 = ['' for count in range(0, 9)]   # example for a list of known length

        # Loop over the flattened classification records
        for row in r:
            # read a row and pullout the flattened data fields we need to aggregate, or pass through.
            new_subject = row['subject_ids']
            new_user = row['user_name']
            field_1 = json.loads(row[field_1_name])  # complex fields must be loaded as json objects
            field_3 = row[field_3_name]
            field_4 = json.loads(row[field_4_name])

            # test for a change in the selector - in this case the selector is the subject
            if new_subject != subject:
                if i != 1:  # if not the first line, we have aggregated all the classifications for
                    # this subject and we can analyse the aggregated fields and output the results.
                    process_aggregation(subject, i, bin_1, bin_3, bin_4)

                # To star the next group, reset the selector, those things we need to pass through,
                # and the bins for the next aggregation.
                i = 1
                subject = new_subject
                users = {new_user}
                # or image_size that is tied to the subject_ids.
                bin_1 = field_1
                bin_3 = field_3
                bin_4 = field_4

            else:
                # The selector has not yet changed so we continue the aggregation:
                # First test for multiple classifications by the same user on this subject, and 
                # if we want to use a fixed number of classifications and a few subjects have 
                # more than the retirement limit (here set at 15).
                if users != users | {new_user} and i <= 15: 
                    users |= {new_user}
                    for count in range(0, len(field_1)):  # summing the elements of a answer_vector
                        bin_1[count] += field_1[count]
                    if len(field_3) > 0:
                        field_3 = field_3 + ';'
                    bin_3 = bin_3 + field_3 # typical aggregation for a field which can be summed
                    i += 1

                    for count in range(0, len(field_4)):  # summing the elements of a answer_vector
                        bin_4[count] += field_4[count]

        # catch and process the last aggregated group
        process_aggregation(subject, i, bin_1, bin_3, bin_4)
