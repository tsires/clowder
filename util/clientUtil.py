import hashlib
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

#creates a local node object. This is a local replica of the node in zookeeper
#contains the name of the node, the stat object of the node, the nodes data, and a list of child nodes
class localNode():
    def __init__(self,inName, inStat, inData):        
        self.name=inName
        self.stat=inStat
        self.data=inData
        self.childNodeList=None




#recursively gets a list of the full directory tree as STRING PATH NAMES 
#i.e. simply returns a list of all the node names in zookeeper under the parameter rootnode
#usage: getFullDirectoryTreeAsString("/", [] , kazooInstance)
#returns:  example ["/", "/dir","/dir/stuff","/dir/stuff2", "/store","/store/stuff"]
def getFullDirectoryTreeAsString(rootNode, returnList, kazooInstance):
    returnList.append(rootNode)

    try:
        nodeList = kazooInstance.get_children(rootNode)
    except NoNodeError:
        return


    for node in nodeList:
        fullNodePath=rootNode + "/" + node 
        getDirectoryTree(fullNodePath, returnList,kazooInstance)

    return returnList


#recursively gets a list of the full directory tree as NODES
#same usage as above hoever returns a list of the local node class defined above
#this contains much more information than the above getFullDirectoryTreeAsString class
def getFullDirectoryTreeAsLocalNodes(rootNode, returnList, kazooInstance):
    
    data, stat =kazooInstance.get(rootNode)
    newNode = localNode(rootNode, stat, data) 
  

    try:
        nodeList = kazooInstance.get_children(rootNode)
    except NoNodeError:
        returnList.append(newNode)
        return

    newNode.childNodeList = nodeList
    returnList.append(newNode)


    for node in nodeList:
        fullNodePath=rootNode + "/" + node 
        getDirectoryTree(fullNodePath, returnList,kazooInstance)

    return returnList


def fileToSha1ChunkArray(filePath, chunkSize):
    outlist =[]
    with open(filePath, 'rb') as theFile:
        while True:
            data = theFile.read(chunkSize)
            if not data:
                break
            hashedData = hashlib.sha1(data).hexdigest()
            outlist.append(hashedData)
    return outlist
      


def addFileAndMetaDataToZookeeper(filePath, chunkKeyArray, kazooInstance):
    

    if not kazooInstance.exists(filePath):
        kazooInstance.create(filePath)
    else:
        return


    transaction = kazooInstance.transaction()
    for chunkValue in chunkKeyArray:
        chunkPath = filePath + "/" + chunkValue
        transaction.create(chunkPath)

    results = transaction.commit()
      


