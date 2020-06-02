import boto3
import configparser
     
if __name__=='__main__':
    
    # load DWH Params from a file
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    DWH_CLUSTER_IDENTIFIER  =   config.get('DWH','DWH_CLUSTER_IDENTIFIER')
    
    # create redshift server
    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    
    redshift.delete_cluster( ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    
    myClusterProps = redshift.describe_clusters(ClusterIdentifier='dwhcluster')['Clusters'][0]
    print(myClusterProps['ClusterStatus'])