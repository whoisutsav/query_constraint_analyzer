#!/bin/bash

#for appname in diaspora discourse falling-fruit fulcrum gitlabhq lobsters mastodon onebody openstreetmap-website redmine ror_ecommerce tracks; do
for appname in dev.to helpy huginn chatwoot solidus fat_free_crm errbit hound hours; do
	eval "ruby analyzer.rb ${appname} > ~/tmp/analyzed_output_${appname}"
done	
