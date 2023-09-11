"""
Order of business

1. Parse the query and figure out the tokens and their types
2. Figure out what templates to search, and what variables to search for for each template
For example, in the query 'loading blk_324234', for some templates the query might match entirely, for some templates
we might match 'loading' in the template and thus have to search for blk_324234, other templates we have to search for both,
other templates we search for neither since there are no two consecutive variables.

"""