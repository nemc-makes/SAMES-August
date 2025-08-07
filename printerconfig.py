# In MES-Demo July.24/printerconfig.py

def get_printers():
    return {
        1: {'name': 'Lucy Caracol 01', 'material': 'PETG', 'technology': 'LFAM', 'rack': '1', 'model': 'Caracol HF'},

        5: {'name': 'Core 001', 'material': 'Fiberon', 'technology': 'FDM', 'rack': '2', 'model': 'Core One'},
        6: {'name': 'Core 002', 'material': 'Fiberon', 'technology': 'FDM', 'rack': '2', 'model': 'Core One'},
        7: {'name': 'Core 003', 'material': 'Fiberon', 'technology': 'FDM', 'rack': '2', 'model': 'Core One'},
        8: {'name': 'Core 004', 'material': 'PETG', 'technology': 'FDM', 'rack': '2', 'model': 'Core One'},
        9: {'name': 'Core 005', 'material': 'PETG', 'technology': 'FDM', 'rack': '2', 'model': 'Core One'},
        10: {'name': 'Core 006', 'material': 'PETG', 'technology': 'FDM', 'rack': '2', 'model': 'Core One'},

        11: {'name': 'XL 001', 'material': 'PETG', 'technology': 'FDM', 'rack': '3', 'model': 'XL'}, # Assuming XL models map to Prusa XL
        12: {'name': 'XL 002', 'material': 'PETG', 'technology': 'FDM', 'rack': '3', 'model': 'XL'},
        13: {'name': 'XL 003', 'material': 'PETG', 'technology': 'FDM', 'rack': '3', 'model': 'XL'},
        14: {'name': 'XL 004', 'material': 'PETG', 'technology': 'FDM', 'rack': '3', 'model': 'XL'},
    }
PRINTER_LABELS = {pid: data["name"] for pid, data in get_printers().items()}