(ns rita-map-kg.schema
  (:require [rita-map-kg.utils :as utils]
            [clojure.string :as str]))

(defn take-first-part [s]
  (first (str/split s #"_")))

;; Function to process areas and extract nodes
(defn process-area [id details]
  (let [{:keys [initial]} details
        {:keys [args pclass]} initial
        coordinates (take 6 args)
        name (utils/clean-name (nth args 6)) 
        ;; Extract field-ref and the list of connection names
        field-ref (nth args 7 nil)
        connections (if (and (map? field-ref) (= ":field-ref" (:type field-ref)))
                      (:names field-ref)
                      [])] ;; Extract connections from :field-ref if present
    ;; Return the node with connections
    {:label (take-first-part pclass) 
     :properties {:nodeid id
                  :coordinates coordinates
                  :name name
                  :connections connections}}))

;; Function to create relationships based on connection info
(defn process-connection-info [id details data]
  (let [{:keys [args pclass]} (:initial details)
        coordinates (take 6 args)
        lvar (nth args 7)]
    (when (map? lvar)
      (let [{:keys [type names]} lvar]
        (when (= type ":field-ref")
          (map (fn [name]
                 (let [{:keys [source target]} (utils/extract-source-target (utils/clean-name name))
                       ;; Base relationship map
                       relationship {:source source 
                                     :target target
                                     :relationship_type pclass
                                     :properties {:id name 
                                                  :coordinates coordinates
                                                  }}]
                   relationship))
               names))))))

;; Main function to extract nodes and relationships from the entire file
(defn extract-schema [file-path]
  (let [data (utils/read-file file-path)
        relationships (atom [])
        nodes (atom [])
        connection-pclasses #{"Door" "DoubleDoor" "OpeningDoor" "Extension" "Cliff" "CorridorJoin"}]

    ;; First, process nodes (do not create relationships here)
    (doseq [[id details] data]
      (when (and (:initial details)
                 (:args (:initial details))
                 (not (contains? connection-pclasses (:pclass (:initial details)))))
        ;; Process and collect node data
        (swap! nodes conj (process-area id details))))

    ;; Then, process relationships from valid connection entries
    (doseq [[id details] data]
      (when (and (:initial details)
                 (contains? connection-pclasses (:pclass (:initial details))))
        (let [relationship-info (process-connection-info id details data)]
          (doseq [relationship relationship-info]
            (when relationship ;; Ensure no nils
              (swap! relationships conj relationship))))))

    ;; Return extracted nodes and relationships
    {:nodes @nodes
     :relationships (distinct @relationships)}))
