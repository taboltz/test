import hailtop.batch as hb
import hail as hl
import re
import subprocess
import os

def vcf2plink(vcf, variant_list, chrom):
    j = b.new_job(name=f'convert2plink-and-subset-{chrom}')  # define job                                                                           

    j.cpu(4).memory("highmem")
    j.storage('32Gi')
    j.image('docker.io/tboltz/py-hail-plink')

    # output handler                                                                                                                                
    j.declare_resource_group(ofile={
        'bed': '{root}.bed',
        'bim': '{root}.bim',
        'fam': '{root}.fam'})

    j.command(f'''cp {vcf['vcf']} /io/file.vcf.gz''')

    j.command(f'''                                                                                                                                  
    curl -L broad.io/install-gcs-connector | python3 -c '                                                                                           
                                                                                                                                                    
import hail as hl                                                                                                                                   
import pandas as pd    
vcf_path = "/io/file.vcf.gz"                                                                                                                        
vcf = hl.import_vcf(vcf_path,force_bgz=True,reference_genome="GRCh38")                                                                              
                                                                                                                                                    
names = vcf["s"].collect()                                                                                                                          
names_new = [name.replace(" ", "-") for name in names]                                                                                              
names_new = [name.replace("_", "-") for name in names]                                                                                              
                                                                                                                                                    
pandas_df = pd.DataFrame({{"s":names,"s_new":names_new}})                                                                                           
                                                                                                                                                    
ht=hl.Table.from_pandas(pandas_df)                                                                                                                  
ht=ht.key_by(ht.s)                                                                                                                                  
                                                                                                                                                    
vcf = vcf.annotate_cols(s_new = ht[vcf.s].s_new)                                                                                                    
                                                                                                                                                    
vcf = hl.variant_qc(vcf)                                              
  vcf_filtered = vcf.filter_rows(vcf.info.INFO[0] < 0.8, keep=False)                                                                                  
                                                                                                                                                    
db = hl.experimental.DB(region="us", cloud="gcp")                                                                                                   
                                                                                                                                                    
vcf_rsids = db.annotate_rows_db(vcf_filtered, "dbSNP_rsid")                                                                                         
                                                                                                                                                    
plink_path="/io/neuroGAP_temp"                                                                                                                      
hl.export_plink(vcf_rsids,output=plink_path,ind_id=vcf_rsids["s_new"],varid=(vcf_rsids["dbSNP_rsid"].rsid[0]))                                      
'                                                                                                                                                   
    ''')
    j.command(f'''plink --bfile /io/neuroGAP_temp --extract {variant_list}  --make-bed --out {j.ofile}  ''')

    return j

if __name__ == '__main__':

    backend = hb.ServiceBackend(billing_project='neale-pumas-bge',
                                remote_tmpdir='gs://hail-batch-temp/',regions=['us-central1'],
                                gcs_requester_pays_configuration='neale-pumas-bge')  # set up backend                                               

    b = hb.Batch(backend=backend, name=f'convert-bcf2plink-and-subset') # define batch                                                              

    variants = b.read_input('gs://neurogap-bge-imputed-regional/prscs/rsids_1kg_hm3_afr.txt')

    #path = "gs://neurogap-bge-imputed-regional/glimpse2/merged_bcfs/"                                                                              
    path = "gs://neurogap-bge-imputed-regional/vcfs/"

    for n in range(22,23):

        vcf=b.read_input_group(
            vcf=f'{path}NeuroGAP_impted_subset_chr{n}.vcf.gz',
            csi=f'{path}NeuroGAP_impted_subset_chr{n}.vcf.gz.csi')

        final_fname = f"NeuroGAP_impted_subset_chr{n}"

        run_plink = vcf2plink(vcf, variants, f'{n}')

        b.write_output(run_plink.ofile, f'gs://neurogap-bge-imputed-regional/prscs/{final_fname}')

    b.run(wait=False) # run batch                                                                                                                   

    backend.close()   # close a batch backend         
