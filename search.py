# from elasticsearch import Elasticsearch

# # Connect to local Elasticsearch
# es = Elasticsearch("http://localhost:9200")

# # Index name
# INDEX_NAME = "local_texts"

# # 1. Create the index (if it doesn't exist)
# if not es.indices.exists(index=INDEX_NAME):
#     es.indices.create(index=INDEX_NAME)
#     print(f"Index '{INDEX_NAME}' created!")

# # 2. Index sample data
# documents = [
#     {"id": 1, "text": "I am a 16-year-old boy who loves playing sports, especially basketball and football."},
#     {"id": 2, "text": "I live in Dhaka, the capital city of Bangladesh, which is known for its bustling streets and rich culture."},
#     {"id": 3, "text": "How are you today? I hope everything is going well in your life and you're staying healthy."},
#     {"id": 4, "text": "I enjoy playing football every weekend with my friends at the local park. It's a great way to stay active."},
#     {"id": 5, "text": "My favorite food is pizza, especially the ones with extra cheese and various toppings like olives and mushrooms."},
#     {"id": 6, "text": "I love traveling around the world, experiencing new cultures, and tasting local cuisines. My dream destination is Japan."},
#     {"id": 7, "text": "I am learning Python programming through online courses and hands-on projects. It's challenging but rewarding."},
#     {"id": 8, "text": "I want to become a software engineer and work in the field of artificial intelligence to solve real-world problems."},
#     {"id": 9, "text": "The weather is nice today. It's sunny with a gentle breeze, perfect for a walk in the park or spending time outdoors."},
#     {"id": 10, "text": "I have two siblings, one older brother and a younger sister, and we always have fun playing board games together."},
#     {"id": 11, "text": "I am a fan of superhero movies, especially the Marvel Cinematic Universe. My favorite superhero is Spider-Man."},
#     {"id": 12, "text": "I like reading books in my free time, particularly science fiction and fantasy novels. J.R.R. Tolkien is my favorite author."},
#     {"id": 13, "text": "I visited Paris last year with my family. The Eiffel Tower and the Louvre Museum were the highlights of the trip."},
#     {"id": 14, "text": "My favorite sport is cricket. I enjoy both playing and watching matches, particularly the IPL tournament."},
#     {"id": 15, "text": "I want to learn data science because I am fascinated by the potential of using data to make informed decisions and predictions."},
#     {"id": 16, "text": "My hobbies include painting, photography, and hiking. I often take pictures during my hikes to capture the beauty of nature."},
#     {"id": 17, "text": "I recently started learning Spanish. It's a beautiful language, and I'm looking forward to traveling to Spain one day."},
#     {"id": 18, "text": "In the future, I want to develop a mobile app that helps people find nearby community events and social gatherings."},
#     {"id": 19, "text": "I enjoy volunteering at animal shelters. It's heartwarming to see the positive impact on the lives of stray animals."},
#     {"id": 20, "text": "I'm currently working on a project to build a personal website that showcases my programming skills and past projects."}
# ]


# for doc in documents:
#     es.index(index=INDEX_NAME, id=doc["id"], document=doc)
#     print(f"Document {doc['id']} indexed!")

# # 3. Search for a term
# search_term = "personal live walk"
# response = es.search(index=INDEX_NAME, query={"match": {"text": search_term}})

# # 4. Display search results
# print(f"\nSearch results for '{search_term}':")
# for hit in response["hits"]["hits"]:
#     print(f"ID: {hit['_id']}, Text: {hit['_source']['text']}")






# from elasticsearch import Elasticsearch

# # Connect to local Elasticsearch
# es = Elasticsearch("http://localhost:9200")

# # Index name
# INDEX_NAME = "local_texts"

# # 1. Create the index (if it doesn't exist)
# if not es.indices.exists(index=INDEX_NAME):
#     es.indices.create(index=INDEX_NAME)
#     print(f"Index '{INDEX_NAME}' created!")

# # 2. Input the search text directly
# input_text = "Donald Trump"

# # 3. Search for each word in the input text in Elasticsearch
# found = []

# # Split the input text into individual words
# words_to_search = input_text.split()

# for word in words_to_search:
#     search_term = word
#     response = es.search(index=INDEX_NAME, query={"match": {"text": search_term}})
    
#     # If any hits are found, the word exists in the Elasticsearch index
#     if response["hits"]["total"]["value"] > 0:
#         found.append(word)

# # 4. Print the found words
# if found:
#     print("Found words in Elasticsearch:")
#     print(", ".join(found))
# else:
#     print("No words from your input text found in Elasticsearch.")



from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch("http://localhost:9200")

# Index name
INDEX_NAME = "local_texts"

# Define the index mapping with keyword type for exact matches
index_mapping = {
    "mappings": {
        "properties": {
            "text": {"type": "keyword"}
        }
    }
}

# 1. Create the index (if it doesn't exist) with the defined mapping
if not es.indices.exists(index=INDEX_NAME):
    es.indices.create(index=INDEX_NAME, body=index_mapping)
    print(f"Index '{INDEX_NAME}' created with keyword mapping!")

# 2. Index a document containing multiple phrases
doc = {
    "text": '''American voters will face a clear choice for president on election day, between Democratic Vice-President Kamala Harris and Republican Donald Trump. Here's a look at what they stand for and how their policies compare on different issues. Harrishas said her day-one priority would be trying to reduce food and housing costs for working families. She promises to ban price-gouging on groceries, help first-time home buyers and provide incentives to increase housing supply. Inflation soared under the Biden presidency, as it did in many western countries, partly due to post-Covid supply issues and the Ukraine war. It has fallen since. Trumphas promised to “end inflation and make America affordable again”. He has promised to deliver lower interest rates, something the president does not control, and he says deporting undocumented immigrants will ease pressure on housing. Economists warn that his vow to impose higher tax on imports could push up prices. Harriswants to raise taxes on big businesses and Americans making $400,000 (£305,000) a year. But she has also unveiled a number of measures that would ease the tax burden on families, including an expansion of child tax credits. She has broken with Biden over capital gains tax, supporting a more moderate rise from 23.6% to 28% compared with his 44.6%. Trumpproposes a number of tax cuts worth trillions, including an extension of his 2017 cuts which mostly helped the wealthy. He says he will pay for them through higher growth and tariffs on imports. Analysts say both tax plans will add to the ballooning deficit, but Trump’s by more. Harrishas made abortion rights central to her campaign, and she continues to advocate for legislation that would enshrine reproductive rights nationwide. Trumphas struggled to find a consistent message on abortion. The three judges he appointed to the Supreme Court while president were pivotal in overturning the constitutional right to an abortion, a 1973 ruling known as Roe v Wade. Harriswas tasked with tackling the root causes of the southern border crisis and helped raise billions of dollars of private money to make regional investments aimed at stemming the flow north. Record numbers of people crossed from Mexico at the end of 2023 but the numbers have fallen since. In this campaign, she has toughened her stance and emphasised her experience as a prosecutor in California taking on human traffickers. Trumphas vowed to seal the border by completing the construction of a wall and increasing enforcement. But he urged Republicans to ditch a hardline, cross-party immigration bill, backed by Harris. He has also promised the biggest mass deportation of undocumented migrants in US history. Experts told the BBC this would face legal challenges. Harrishas vowed to support Ukraine “for as long as it takes”. She has pledged, if elected, to ensure the US and not China wins "the competition for the 21st Century". She has been a longtime advocate for a two-state solution between the Israelis and Palestinians, and has called for an end to the war in Gaza. Trumphas an isolationist foreign policy and wants the US to disentangle itself from conflicts elsewhere in the world. He has said he would end the war in Ukraine in 24 hours through a negotiated settlement with Russia, a move that Democrats say would embolden Vladimir Putin. Trump has positioned himself as a staunch supporter of Israel but said little on how he would end the war in Gaza. Harrishas criticised Trump’s sweeping plan to impose tariffs on imports, calling it a national tax on working families which will cost each household $4,000 a year. She is expected to have a more targeted approach to taxing imports, maintaining the tariffs the Biden-Harris administration introduced on some Chinese imports like electric vehicles. Trumphas made tariffs a central pledge in this campaign. He has proposed new 10-20% tariffs on most foreign goods, and much higher ones on those from China. He has also promised to entice companies to stay in the US to manufacture goods, by giving them a lower rate of corporate tax. Harris, as vice-president, helped pass the Inflation Reduction Act, which has funnelled hundreds of billions of dollars to renewable energy, and electric vehicle tax credit and rebate programmes. But she has dropped her opposition to fracking, a technique for recovering gas and oil opposed by environmentalists. Trump, while in the White House, rolled back hundreds of environmental protections, including limits on carbon dioxide emissions from power plants and vehicles. In this campaign he has vowed to expand Arctic drilling and attacked electric cars. Harrishas been part of a White House administration which has reduced prescription drug costs and capped insulin prices at $35. Trump, who has often vowed to dismantle the Affordable Care Act, has said that if elected he would only improve it, without offering specifics. The Act has been instrumental in getting health insurance to millions more people. He has called for taxpayer-funded fertility treatment, but that could be opposed by Republicans in Congress. Harrishas tried to contrast her experience as a prosecutor with the fact Trump has been convicted of a crime. Trumphas vowed to demolish drugs cartels, crush gang violence and rebuild Democratic-run cities that he says are overrun with crime. He has said he would use the military or the National Guard, a reserve force, to tackle opponents he calls "the enemy within" and "radical left lunatics" if they disrupt the election. Harrishas made preventing gun violence a key pledge, and she and Tim Walz - both gun owners - often advocate for tighter laws. But they will find that moves like expanding background checks or banning assault weapons will need the help of Congress. Trumphas positioned himself as a staunch defender of the Second Amendment, the constitutional right to bear arms. Addressing the National Rifle Association in May, he said he was their best friend. Harrishas called for the decriminalisation of marijuana for recreational use. She says too many people have been sent to prison for possession and points to disproportionate arrest numbers for black and Latino men. Trumphas softened his approach and said it's time to end "needless arrests and incarcerations" of adults for small amounts of marijuana for personal use. The rapper himself introduced the former US president at a presidential campaign rally in his home town. Video captures the moment a car boot is opened revealing dozens of allegedly stolen campaign yard posters. The BBC's Lily Jamali explains the backstory behind the former president's campaign stunt. Kristine Fishell was the second winner of Musk’s giveaway at a town hall event in Pennsylvania on Sunday night. The US presidential candidates continued to campaign across key swing states on Sunday. Two Pennsylvania farmers have been targeted by false claims and threats after appearing in a Kamala Harris advert. The event has historically featured the exchange of jabs between presidential candidates. This is the reality for American astronauts Sunita Williams and Barry Wilmore who will be voting from space. When asked if he'd spoken to Putin since leaving office, the former US president said: "If I did, it's a smart thing". The BBC's Nada Tawfik reports from the battleground state, where officials say record numbers turned out to cast their ballots. Politicians and pundits were quizzed by the BBC's Fiona Bruce and American voters in Pennsylvania. The BBC's analysis editor examines the security failures that led to the former president being shot at during a rally in July. BBC Verify analysed a false post claiming to show a pro-abortion campaign ad that has widely circulated on social media. BBC Monitoring's Luis Fajardo looks at how the Mexican media has been reporting on US elections. BBC Monitoring's Luis Fajardo looks at how the Mexican media has been reporting on US elections. The BBC's North America correspondent Anthony Zurcher says the vice-presidential candidates found common ground. The BBC asks college students what they thought of JD Vance and Tim Walz’s debate performance. Politeness, policy and a few tense moments - watch how Vance and Walz tackled the 90-minute event. When questioned about saying he was in Hong Kong during the Tiananmen Square massacre, Walz says he "misspoke". It comes after Vance was fact-checked on a statement about "millions of illegal immigrants". From abortion to trade, here are his policy pledges as he runs for the White House for a third time. Sir Keir Starmer says his party sends volunteers to the US for 'pretty much every' presidential election. Emails from the Republican National Committee claim 2.7 million immigrants will vote - but the real figure is much different. Harris answered questions about Joe Biden's dropping out of the race and whether she would pardon Donald Trump. Labour activists volunteering in the US presidential election amounts to illegal "contributions", the complaint alleges. Copyright 2024 BBC. All rights reserved.TheBBCisnot responsible for the content of external sites.Read about our approach to external linking. '''
}

es.index(index=INDEX_NAME, body=doc)
print("Document indexed!")

# 3. Search for multiple terms (e.g., "president", "Kamala Harris", "Donald Trump")
response = es.search(index=INDEX_NAME, body={
    "query": {
        "bool": {
            "should": [
                {"match_phrase": {"text": "sams"}},
                {"match_phrase": {"text": "trump"}},
                {"match_phrase": {"text": "sams"}}
            ],
            "minimum_should_match": 1  # At least one of these terms should match
        }
    }
})

# 4. Check if the search returned any results
if response['hits']['total']['value'] > 0:
    print("Found one of the phrases in the documents!")
else:
    print("No results found for these phrases.")
