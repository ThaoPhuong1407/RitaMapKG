(ns rita-map-kg.schema
  (:require [rita-map-kg.utils :as utils]
            [clojure.string :as str]))

(defn take-first-part [s]
  (first (str/split s #"_")))

(defn get-coordinates [mymap key]
  (get-in mymap [key :properties :coordinates]))


(defn compare-areas [coords1 coords2]
  (let [[x1 z1 _ _ _ _] coords1  
        [x3 z3 _ _ _ _] coords2  
        x-diff (- x3 x1)
        z-diff (- z3 z1)
        x-dir (cond
                (pos? x-diff) "E"
                (neg? x-diff) "W"
                :else nil)
        z-dir (cond
                (pos? z-diff) "S"
                (neg? z-diff) "N"
                :else nil)]
    (str (when z-dir (str z-dir)) 
         (when x-dir (str x-dir)))))

;; Function to process areas and extract nodes
(defn process-area [id details]
  (let [{:keys [initial]} details
        {:keys [args pclass]} initial
        coordinates (take 6 args)
        name (utils/clean-name (nth args 6)) 
        ;; Collect all field-ref connections
        connections (->> args
                         (filter map?) ;; Filter only maps in the args
                         (filter #(= ":field-ref" (:type %))) 
                         (mapcat :names))] ;; Extract all :names and flatten the list
    {:label (take-first-part pclass) 
     :properties {:nodeid id
                  :coordinates coordinates
                  :name name
                  :connections connections}}))

;; Function to create relationships based on connection info
(defn process-connection-info [details nodes]
  (let [{:keys [args pclass]} (:initial details)
        coordinates (take 6 args)
        lvar (nth args 7)]
    (when (map? lvar)
      (let [{:keys [type names]} lvar]
        (when (and (= type ":field-ref") (seq names))
          (let [name (first names)
                {:keys [source target]} (utils/extract-source-target (utils/clean-name name))
                source-cord (get-coordinates nodes (keyword source))
                target-cord (get-coordinates nodes (keyword target)) source-direction (compare-areas target-cord source-cord)
                target-direction (compare-areas source-cord target-cord)
                relationship {:source source 
                              :target target
                              :relationship_type pclass
                              :properties {:id name 
                                           :coordinates coordinates
                                           :is-bidirectional true
                                           :description (str source-direction " " pclass " " target-direction)}}]
            relationship))))))


;; Main function to extract nodes and relationships from the entire file
(defn extract-schema [file-path]
  (let [data (utils/read-file file-path)
        relationships (atom {})
        nodes (atom {})
        connection-pclasses #{"Door" "DoubleDoor" "OpeningDoor" "Extension" "Cliff" "CorridorJoin"}]

    ;; First, process nodes 
    (doseq [[id details] data]
      (when (and (:initial details)
                 (:args (:initial details))
                 (not (contains? connection-pclasses (:pclass (:initial details)))))
        (swap! nodes assoc id (process-area id details)))) 

    ;; Then, process relationships from valid connection entries
    (doseq [[_id details] data]
      (when (and (:initial details)
                 (contains? connection-pclasses (:pclass (:initial details))))
        (let [relationship (process-connection-info details @nodes)]
            (when relationship ;; Ensure no nils  
              (swap! relationships assoc (:id (:properties relationship)) relationship)))))

    ;; Return extracted nodes and relationships 
    {:nodes @nodes
     :relationships @relationships}))
