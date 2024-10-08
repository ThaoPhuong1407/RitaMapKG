(ns rita-map-kg.core
  (:gen-class)
  (:require [rita-map-kg.schema :refer [extract-schema]]
            [rita-map-kg.rmq :refer [init-python init-rmq-instance send-rmq-message close-rmq-connection]]
            [rita-map-kg.utils :as utils]
            [cheshire.core :as json]))
            

(def config
  {:host "localhost"
   :port 5672
   :exchange "plant_exchange"
   :routing-key "observations"})

(defn -main
  [& args]
  (println "Starting RMQ message test...")
  (init-python) ;; Initialize Python environment
  (init-rmq-instance config) ;; Initialize RMQ instance
  
  ;; Read and extract nodes and relationships from file
  (let [file-path "./resources/dragon_2.0.json"
        data (extract-schema file-path)
        nodes (:nodes data)
        relationships (:relationships data)]

    ;; Send nodes to RMQ
    (doseq [node nodes]
      (let [node-data (second node)]
        (send-rmq-message config
                          {:state "create-node"
                           :label (:label node-data)
                           :properties (:properties node-data)})))

    ;; Send relationships to RMQ
    (doseq [rel relationships]
      (let [rel-data (second rel)]
        (send-rmq-message config
                          {:state "create-relationship"
                           :start_property_key "nodeid"
                           :start_property_value (:source rel-data)
                           :end_property_key "nodeid"
                           :end_property_value (:target rel-data)
                           :relationship_type (:relationship_type rel-data)
                           :relationship_properties (:properties rel-data)}))))

  ;; Close the RMQ connection
  (close-rmq-connection)
  (println "Messages sent and RMQ connection closed."))
