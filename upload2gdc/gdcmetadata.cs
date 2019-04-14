using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;

namespace upload2gdc
{
    public class SUR  // slimed down version of submitted_unaligned_reads
    {
        public string submitter_id { get; set; }
        public string file_name { get; set; }
        public long file_size { get; set; }
        public string md5sum { get; set; }
        public string project_id { get; set; }
    }

    class GDCmetadata
    {
        public static List<GDCjson> gdc_jsonObjects = new List<GDCjson>();                      // list into which GDC json is deserialized 
        public static Dictionary<string, SUR> SURdictionary = new Dictionary<string, SUR>();    // container for submitted unaligned reads entities
        // public static List<ReadGroup> ReadGroupList = new List<ReadGroup>();                 // list for read_group objects - do not need at this time

        public static bool LoadGDCJsonObjects(string inputString)
        {
            try
            {
                gdc_jsonObjects = JsonConvert.DeserializeObject<List<GDCjson>>(inputString, new GDCConverter());
            }
            catch
            {
                return false;
            }

            SURdictionary.Clear();
            foreach (GDCjson gdcJsonOject in gdc_jsonObjects)
            {
                if (gdcJsonOject.type == "submitted_unaligned_reads")
                {
                    SubmittedUnalignedReads temp1 = new SubmittedUnalignedReads();
                    temp1 = gdcJsonOject as SubmittedUnalignedReads;

                    SUR temp2 = new SUR()
                    {
                        submitter_id = temp1.submitter_id,
                        file_name = temp1.file_name,
                        file_size = temp1.file_size,
                        md5sum = temp1.md5sum,
                        project_id = temp1.project_id
                    };

                    SURdictionary.Add(temp1.submitter_id, temp2);
                } 
                // *** at this time we have no use for read_group objects, so do not load them
                //else if (gdcJsonOject.type == "read_group")
                //{
                //    ReadGroup temp1 = new ReadGroup();
                //    temp1 = gdcJsonOject as ReadGroup;
                //    ReadGroupList.Add(temp1);
                //}
            }

            gdc_jsonObjects.Clear();
            return true;
        }

    }



    // these next 5 classes are based on GDC entities: 
    // https://docs.gdc.cancer.gov/Data_Dictionary/viewer/

    public class GDCjson
    {
        public string type { get; set; }
    }

    public class Aliquots
    {
        public string submitter_id { get; set; }
    }

    public class ReadGroup : GDCjson
    {
        public string submitter_id { get; set; }
        public string experiment_name { get; set; }
        public bool is_paired_end { get; set; }
        public string library_name { get; set; }
        public string library_strategy { get; set; }
        public string platform { get; set; }
        public string read_group_name { get; set; }
        public int read_length { get; set; }
        public string sequencing_center { get; set; }
        public Aliquots aliquots { get; set; }
        public string size_selection_range { get; set; }
        public string adapter_sequence { get; set; }
        public string library_strand { get; set; }
        public string library_preparation_kit_name { get; set; }
        public string base_caller_version { get; set; }
        public string base_caller_name { get; set; }
        public string spike_ins_concentration { get; set; }
        public string sequencing_date { get; set; }
        public string spike_ins_fasta { get; set; }
        public string library_selection { get; set; }
        public string library_preparation_kit_vendor { get; set; }
        public string project_id { get; set; }
        public string instrument_model { get; set; }
        public bool includes_spike_ins { get; set; }
        public string flow_cell_barcode { get; set; }
    }

    public class read_groups
    {
        public string submitter_id { get; set; }
    }

    public class SubmittedUnalignedReads : GDCjson
    {
        public string submitter_id { get; set; }
        public string data_category { get; set; }
        public string data_format { get; set; }
        public string data_type { get; set; }
        public string experimental_strategy { get; set; }
        public string file_name { get; set; }
        public long file_size { get; set; }
        public string md5sum { get; set; }
        public string project_id { get; set; }
        public List<read_groups> read_group { get; set; }
    }


    // custom converter required since the objects are of multiple types
    // need to determine the type then deserialize into apprpriate object
    public class GDCConverter : JsonCreationConverter<GDCjson>
    {
        protected override GDCjson Create(Type objectType, JObject jObject)
        {
            if (jObject["type"].Value<string>() == "read_group")
                return new ReadGroup();

            else if (jObject["type"].Value<string>() == "submitted_unaligned_reads")
                return new SubmittedUnalignedReads();

            else
                throw new NotImplementedException();
        }
    }


    public abstract class JsonCreationConverter<T> : JsonConverter
    {
        protected abstract T Create(Type objectType, JObject jObject);

        public override bool CanConvert(Type objectType)
        {
            return typeof(T).IsAssignableFrom(objectType);
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            // Load JObject from stream
            JObject jObject = JObject.Load(reader);

            // Create target object based on JObject
            T target = Create(objectType, jObject);

            // Populate the object properties
            serializer.Populate(jObject.CreateReader(), target);

            return target;
        }

        public override void WriteJson(JsonWriter writer,
            object value, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }
    }



}
