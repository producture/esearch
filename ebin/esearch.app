{application, esearch, 
	[
	 {description, "application desc"},
  	 {vsn,"0.1.0"},
  	 {modules,[esearch_app, esearch_sup, esearch]},
  	 {registered, [esearch_sup]},
   	 {applications, [kernel, stdlib]},
  	 {mod, {esearch_app, []}}
	]}.
