from email.mime import base
import threading
import collections
from collections import OrderedDict
from traceback import print_tb

# we will receive copies of pages inside base pages and tail pages that need to be merged
# this set will always include primary key page to uniquely identify a record
def merge(page_directory, pages):
    
    # to store base rid values of all records that we accessed one by one in the tail page
    # this way, because we iterate thru a tail page in the reverse order
    # we store that record's rid in this list
    # if we encouter a past update for a record that is in this list, we ignore it
    base_rids_accessed = []
    base_pages = OrderedDict()
    tail_pages = OrderedDict()

    # Collect all base pages and tail pages
    for page in pages:
        # store the page id 
        page_id = page.page_id
        # split string with - as delimiter
        split_string = page_id.split("-")
        # get the page type - 'b' or 't'
        page_type = split_string[0][0]
        # get the base page/ tail page no
        main_page_no = split_string[1]
        page_no = split_string[2]

        # does not matter if the base page exists in the dictionary or not, will get added
        if page_type == 'b':
            if main_page_no not in base_pages.keys():
                base_pages[main_page_no] = {}
            base_pages[main_page_no][page_no] = page
        else:
            if main_page_no not in tail_pages.keys():
                tail_pages[main_page_no] = {}
            tail_pages[main_page_no][page_no] = page

    # the tail page with the largest number as id
    num_tail_pages = len(tail_pages)

    # assume baseRid column is always sent
    for tail_page_no, pages in reversed(tail_pages.items()):
        all_pages_in_order = collections.OrderedDict(sorted(pages.items()))
        all_keys = all_pages_in_order.keys()
        max_key_value = max(all_keys)
        base_rid_page = all_pages_in_order[max_key_value]

        slot_no_list = []
        max_no = base_rid_page.num_records
        for i in range(max_no):
            slot_no_list.append(i)

        num_tail_pages = len(tail_pages)
    
        for curr_slot_no in reversed(slot_no_list):
            base_rid = base_rid_page.read(curr_slot_no)
            # if this record was accessed previosuly in the tail page, no need to work with any past updates
            if base_rid in base_rids_accessed:
                pass
            # if this record's base_rid does not exist in the list of base_rids_accessed
            else:
                # add it to the list of base_rids_accessed
                base_rids_accessed.append(base_rid)
                # get the location info from rid 
                base_page_range, base_page, base_slot = page_directory[base_rid]
                all_base_page_pages = {}
                for key, value in base_pages.items():
                    key_int = int(key)
                    if key_int == base_page:
                        all_base_page_pages = value
                # to store the key for the baseRid column so we can skip it, we don't update tail page's Base Rid column
                max_key = (max(k for k, v in all_pages_in_order.items()))
                for single_page_no, single_page in all_pages_in_order.items():
                    # to skip the last baseRid column
                    if single_page_no == max_key: 
                        pass
                    else:
                        # read the updated value from the tail slot
                        val = single_page.read(curr_slot_no)
                        # replace the old base slot value with this tail slot's value
                        #
                        # all_base_page_pages[single_page_no].overwrite(base_slot, val)
                        # 
                        for key, value in all_base_page_pages.items():
                            if key == single_page_no:
                                value.overwrite(base_slot, val)

    #to return the largest tail page number
    base_pages[0] = {0: num_tail_pages}

    return base_pages
