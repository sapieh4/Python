import win32com.client

# Opening Excel software using the win32com
File = win32com.client.Dispatch("Excel.Application")

# Optional line to show the Excel software
File.Visible = 1

# Opening your workbook
Workbook = File.Workbooks.open("C:\\Users\\vp0421\\OneDrive - Bunasta\\OCR Testing.xlsx")

# Refeshing all the shests
Workbook.RefreshAll()

# Saving the Workbook
Workbook.Save()

# Closing the Excel File
File.Quit()