import sys
from PyQt5.QtWidgets import QMainWindow, QApplication, QWidget, QAction, \
    QTableView, QTableWidget,QTableWidgetItem,QVBoxLayout,QTableWidgetSelectionRange
from PyQt5.QtGui import QIcon
from PyQt5.QtCore import pyqtSlot,Qt
import unicodedata



BOX=unicodedata.lookup("BALLOT BOX")
CHECKED=unicodedata.lookup("BALLOT BOX WITH CHECK")

class App(QWidget):
    def __init__(self):
        super().__init__()
        self.title = 'PyQt5 table - pythonspot.com'
        self.left = 0
        self.top = 0
        self.width = 300
        self.height = 200
        self.initUI()
        
    def initUI(self):
        self.setWindowTitle(self.title)
        self.setGeometry(self.left, self.top, self.width, self.height)
        
        self.createTable()

        # Add box layout, add table to box layout and add box layout to widget
        self.layout = QVBoxLayout()
        self.layout.addWidget(self.tableWidget) 
        self.setLayout(self.layout) 

        # Show widget
        self.show()

    def createTable(self):
        # Create table
        self.tableWidget = QTableWidget()
        self.tableWidget.setRowCount(1000)
        self.tableWidget.setColumnCount(4)
        self.tableWidget.setHorizontalHeaderLabels(['','Dir','Source','Dest'])
        for i in range(0,1000):
            item = QTableWidgetItem("foo")
            item.setCheckState(Qt.Checked)
            self.tableWidget.setItem(i,0, item)
        # table selection change
        self.tableWidget.itemSelectionChanged.connect(self.on_changed)
        self.tableWidget.cellPressed.connect(self.on_cell_pressed)
        self.tableWidget.itemClicked.connect(self.on_item_clicked)
        self.newState = None

    @pyqtSlot()
    def on_changed(self):
        print("on_changed")
        # Handles initial click and drag. Sets the entire row
        for r in self.tableWidget.selectedRanges():
            self.tableWidget.setRangeSelected(
                QTableWidgetSelectionRange(r.topRow(),0,
                                           r.bottomRow(),self.tableWidget.columnCount()-1),
                True)
            for row in range(r.topRow(),r.bottomRow()+1):
                item = self.tableWidget.item(row,0)
                if item and self.newState!=None:
                    item.setCheckState(self.newState)
            

    @pyqtSlot(int,int)
    def on_cell_pressed(self,row,column):
        print("on_cell_pressed",row,column)
        self.flipClickState(row)

    @pyqtSlot(int)
    def flipClickState(self,row):
        item = self.tableWidget.item(row,0)
        if item:
            self.newState = Qt.Checked if item.checkState()==Qt.Unchecked else Qt.Unchecked
            item.setCheckState(self.newState)
            
    @pyqtSlot()
    # Called on release; clear the selection
    def on_item_clicked(self,item):
        for range in self.tableWidget.selectedRanges():
            self.tableWidget.setRangeSelected(range,False)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = App()
    sys.exit(app.exec_())  
