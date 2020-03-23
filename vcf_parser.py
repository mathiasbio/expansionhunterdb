#!/apps/bio/software/anaconda2/envs/vilma_general/bin/python

import os
import argparse
from pymongo import MongoClient

#########################################################

parser = argparse.ArgumentParser(prog="automatic_json.py")
requiredNamed = parser.add_argument_group('required arguments')
requiredNamed.add_argument("-v", "--vcf", \
                        help="If uploading vcf files use this flag to add paths")
requiredNamed.add_argument("-o", "--output", \
                        help="New updated vcf with variant frequency (CVF) added to info field")

args = parser.parse_args()

#########################################################
# Parse vcf file and get fields. 
def vcf_parse():
    with open(args.vcf, "r") as vcf_file:
        the_dict = []
        for row in vcf_file:
            if not row.startswith("#"):
                r = row.split('\t')
                r = [string.strip() for string in r]
                try:
                    the_dict.append({"chr":int(r[0]), "pos":int(r[1]), "ref":r[3], "alt":r[4], "name": [os.path.basename(args.vcf)]})
                except:
                    the_dict.append({"chr":r[0], "pos":int(r[1]), "ref":r[3], "alt":r[4], "name": [os.path.basename(args.vcf)]})
        return the_dict


# Add information from vcf to database collection.
def db(mydict):
    client = MongoClient()
    db = client.database
    variants_collection = db.variants_collection

    # Add information from vcf to collection in database.
    if os.path.basename(args.vcf) in variants_collection.distinct("name"):
        print("VCF has already been uploaded")
    else:
        print("adding..")
        for i in mydict:
            variants_collection.update_many({"chr":i["chr"], "pos":i["pos"], "ref":i["ref"], "alt":i["alt"]},
                                        {"$push": {"name": os.path.basename(args.vcf)}},
                                        upsert=True)

#    for i in mydict:
#            variants_collection.insert_many([{"chr":i["chr"], "pos":i["pos"], "ref":i["ref"], "alt":i["alt"], "name":[os.path.basename(args.vcf)]}])

    if args.output:
        # Calculate freq using list of names in documents.
        for i in mydict:
            sel_variants = int(str([i["name"] for i in variants_collection.aggregate([{"$match": {"chr":i["chr"], "pos":i["pos"], "ref":i["ref"], "alt":i["alt"]}}, {"$project": {"name": {"$size":"$name"}}}])]).strip("[]"))
            all_variants = int(str([i["name"] for i in variants_collection.aggregate([{"$project": {"name": {"$size":"$name"}}}, {"$group": {"_id":"$all","name": {"$sum":"$name"}}}])]).strip("[]"))
            frequency = sel_variants/all_variants
            i.update({"freq":frequency})
     
        # Write new output vcf with added variant frequency in info field.
        vcf = open(args.vcf, "r")
        new_vcf = open(args.output, "w+")
        infoheader_list = []
        fileformat = []
        format_list = []
        else_list = []
        info_list = []

        # Divide header and add homemade line for CVF in ##INFO.
        for line in vcf:
            r = line.split('\t')
            r = [string.strip() for string in r]
            if line.startswith("##fileformat"):
                fileformat.append(line)
            if line.startswith("##FORMAT"):
                format_list.append(line)
            if not line.startswith("##fileformat") and not line.startswith("##FORMAT") and not line.startswith("##INFO") and line.startswith("#"):
                else_list.append(line)
            if line.startswith("##INFO"):
                infoheader_list.append(line)
            
            # Calculate frequency and add to CVF=[float] in infofield.
            for i in mydict:
                if str(i["chr"]) in r[0:1] and str(i["pos"]) in r[1:2] and i["ref"] in r[3:4] and i["alt"] in r[4:5]:
                    info = line.split(";")
                    info.insert(1, "CVF="+str(i["freq"]))
                    info_list.append(';'.join(info))
        
        # Append CVF explanation to end of ##INFO block. 
        infoheader_list.append('''##INFO=<ID=CVF,Number=1,Type=Float,Description="Variant frequency, from CGG database (CGG Variant Frequency)">''')

        # Put homemade header together and write new header 
        # + variants with added CVF to a new vcf file.
        header = fileformat+format_list+infoheader_list+else_list
        new_vcf.write(''.join(str(e) for e in header))
        new_vcf.write(''.join(str(e) for e in info_list))

        new_vcf.close()
        vcf.close()


def main():
    vcf_parse()
    mydict = vcf_parse()
    db(mydict)


if __name__ == "__main__":
    main()

