
view_query = '''
set transaction isolation level read uncommitted

SELECT * FROM Salesforce.vwAccountMatchingData
ORDER BY AccountStatusScore asc, ParentScore asc
'''