(ns clinvar-raw.ingest-test
  "Test the clinvar-raw.ingest namespace."
  (:require [clojure.test       :refer [deftest is testing]]
            [clojure.data.json  :as json]
            [clinvar-raw.ingest :as ingest]))

(def ^:private now
  "EDN content of a new message file."
  (-> "./test/clinvar_raw/resources/ingest/20191202-variation.json"
      slurp ingest/decode))

(def ^:private was
  "EDN content of an old message file."
  (-> "./test/clinvar_raw/resources/ingest/20191105-variation.json"
      slurp ingest/decode))

(def ^:private msg
  "EDN for a shorter message to ease testing and eye-balling."
  {"id" "17674"
   "child_ids" []
   "name" "NM_007294.3(BRCA1):c.4065_4068del (p.Asn1355fs)"
   "content"
   {"FunctionalConsequence"
    {"@Value" "functional variant"
     "XRef" {"@DB" "Sequence Ontology"
             "@ID" "SO:0001536"}}
    "HGVSlist" {"HGVS"
                [{"@Type"
                  "coding"
                  "NucleotideExpression"
                  {"@change"    "c.4065_4068del"
                   "Expression" {"$" "U14680.1:c.4065_4068del"}}}
                 {"@Type"
                  "non-coding"
                  "NucleotideExpression"
                  {"@change"    "n.4184_4187delTCAA"
                   "Expression" {"$" "U14680.1:n.4184_4187delTCAA"}}}]}
    "Location" {"CytogeneticLocation" {"$" "17q21.31"}
                "SequenceLocation"    [{"@display_stop"  "41243483"
                                        "@display_start" "41243480"}
                                       {"@display_stop"  "43091466"
                                        "@display_start" "43091463"}]}
    "OtherNameList" {"Name" [{"$" "3333del4"}
                             {"$" "4184_4187delTCAA"}
                             {"$" "4184del4"}]}
    "XRefList" {"XRef" [{"@DB"   "Breast Cancer Information Core (BIC) (BRCA1)"
                         "@ID"   "4184&base_change=del TCAA"}
                        {"@DB"   "ClinGen", "@ID" "CA026492"}
                        {"@DB"   "OMIM"
                         "@ID"   "113705.0015"
                         "@Type" "Allelic variant"}
                        {"@DB"   "dbSNP"
                         "@ID"   "80357508"
                         "@Type" "rs"}]}}
   "descendant_ids" []
   "protein_change" ["N1355fs" "N1308fs"]})

(defn ^:private encode
  "Encode EDN as a JSON string with stringified `content` field to undo
  what ingest/decode does.."
  [edn]
  (-> edn
      (update "content" json/write-str)
      json/write-str))

(deftest round-trip
  (testing "round-tripping through encode and decode does not lose data"
    (is (= msg (ingest/decode (encode msg))))))

(deftest differ-detects-differences
  (testing "differ? is nil when messages are semantically the same"
    (is (nil? (ingest/differ? now was))))
  (testing "differ? is truthy when messages differ semantically"
    (is (not (nil? (ingest/differ? now msg))))))

(defn ^:private ok?
  "True when differ? returns either nil or a hash of its first argument."
  [[now was]]
  (let [ret (ingest/differ? now was)]
    (or (nil? ret)
        (==   ret (hash (#'ingest/disorder now))))))

(deftest differ-returns-nil-or-hash
  (testing "differ? returns nil or the hash of its first argument"
    (let [messages [msg now was]
          args     (for [now messages was messages] [now was])]
      (is (every? true? (map ok? args))))))
