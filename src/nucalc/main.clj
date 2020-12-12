(ns nucalc.main
  (:require [clojure.string :as string]))

(def commands [])

(defn find-command [command-name commands]
  (first (filter (fn [command]
                   (= command-name
                      (name (:name (meta command)))))
                 commands)))

(defn -main [& command-line-arguments]
  (let [[command-name & arguments] command-line-arguments]
    (if-let [command (find-command command-name
                                   commands)]
      (apply command arguments)

      (do (when command-name
            (println (str "Unknown command \"" command-name "\".")))
          (println "Use one of the commands:")
          (println "------------------------")
          (println (->> commands
                        (map (fn [command-var]
                               (str (:name (meta command-var))
                                    ": "
                                    (:arglists (meta command-var))
                                    "\n"
                                    (:doc (meta command-var)))))
                        (string/join "------------------------\n")))))))
