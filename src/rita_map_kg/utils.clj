(ns rita-map-kg.utils
  (:require [clojure.string :as str]
            [cheshire.core :as json]
            [clojure.java.io :as io]))

;; Function to clean leading colons from both namespaced and regular keys
(defn clean-key [key]
  (cond
    ;; Handle namespaced keywords (::access)
    (keyword? key) (-> key name (str/replace #"^:" "") keyword)
    ;; Handle string keys with leading colon (":access")
    (string? key) (-> key (str/replace #"^:" "") keyword)
    ;; Return the key as-is for other cases
    :else key))

;; Function to clean values (removing leading colons)
(defn clean-value [value]
  (if (and (keyword? value) (re-find #"^:" (name value)))
    ;; Convert keyword with colon to normal keyword
    (keyword (str/replace (name value) #"^:" ""))
    value))

;; Function to recursively clean both keys and values in the map
(defn clean-map [m]
  (cond
    (map? m) (into {} (for [[k v] m]
                        [(clean-key k) (clean-map (clean-value v))]))  ;; Recursively clean
    (coll? m) (map clean-map m)  ;; Handle nested collections
    :else m))  ;; Return the value as is for non-map structures

;; Function to read the JSON file and clean keys and values
(defn read-file [file-path]
  (with-open [rdr (io/reader file-path)]
    (let [parsed-data (json/parse-stream rdr true)]
      (clean-map parsed-data))))
       

(defn clean-name [name]
  (-> name
      (clojure.string/replace #"^\"|\"$" "")  ; Remove leading and trailing quotes
      (clojure.string/replace #"\\\"" "\""))) ; Remove escape characters for quotes

;; Example usage:
(clean-name "\"Jungle\"")


;; Helper function to extract source and target for relationships
(defn extract-source-target [lvar-id]
  (let [[source target] (str/split lvar-id #"-")]
    {:source source :target target}))
