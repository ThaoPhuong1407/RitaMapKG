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
      (send-rmq-message config
                        {:state "create-node" 
                         :label (:label node)
                         :properties (:properties node)}))

    ;; Send relationships to RMQ
    (doseq [rel relationships]
      (send-rmq-message config
                        {:state "create-relationship" 
                         :start_property_key "nodeid"
                         :start_property_value (:source rel) 
                         :end_property_key "nodeid"
                         :end_property_value (:target rel)       
                         :relationship_type (:relationship_type rel)
                         :relationship_properties (:properties rel)})))
                         
  ;; Close the RMQ connection
  (close-rmq-connection)
  (println "Messages sent and RMQ connection closed."))
