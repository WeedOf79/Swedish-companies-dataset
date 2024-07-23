# Valerio Malerba, Uppsala, 2024

import pandas as pd
import matplotlib.pyplot as plt
from adjustText import adjust_text

# Retrieve the top n companies with highest increase of feature between the specified years
def Top_growing(dataframe, feature, year1, year2, n, ont):
    # Copy the DataFrame to avoid SettingWithCopyWarning
    subset = dataframe.copy()[[f"{feature}_{year1}", f"{feature}_{year2}", 'organization number']]
    # Calculate the difference between the specified years
    subset[f'Diff_{feature}_{year1}_{year2}'] = subset[f"{feature}_{year2}"] - subset[f"{feature}_{year1}"]
    # Sort companies based on the difference and select the top n
    top_n = subset.sort_values(by=f'Diff_{feature}_{year1}_{year2}', ascending=False).head(n)
    top_n['organization number'] = top_n['organization number'].astype(str).map(ont)
    result = top_n[['organization number', f'Diff_{feature}_{year1}_{year2}']]
    plt.figure(figsize=(10, 6))
    plt.bar(result['organization number'], result[f'Diff_{feature}_{year1}_{year2}'])
    plt.xlabel('Organization Number')
    plt.ylabel(f'Difference in {feature} between {year1} and {year2}')
    plt.title(f'Top {n} Growing Companies')
    plt.xticks(rotation=45, ha='right')  # Label rotation for increased readability
    plt.tight_layout()
    plt.show()
    return result

# Very similar to Top_growing, this other function computes the average value between two years, then selects those n with the highest value
# The results are plotted in a bar graph
def Average(dataframe, feature, year1, year2, n, ont):
    # Copy the DataFrame to avoid SettingWithCopyWarning
    subset = dataframe.copy()[[f"{feature}_{year1}", f"{feature}_{year2}", 'organization number']]
    # Calculate the average between the specified years
    subset[f'Average_{feature}_{year1}_{year2}'] = (subset[f"{feature}_{year2}"] + subset[f"{feature}_{year1}"]) / 2
    # Sort companies based on the difference and select the top n
    top_n = subset.sort_values(by=f'Average_{feature}_{year1}_{year2}', ascending=False).head(n)
    top_n['organization number'] = top_n['organization number'].astype(str).map(ont)
    result = top_n[['organization number', f'Average_{feature}_{year1}_{year2}']]
    plt.figure(figsize=(10, 6))
    plt.bar(result['organization number'], result[f'Average_{feature}_{year1}_{year2}'])
    plt.xlabel('Organization Number')
    plt.ylabel(f'Average in {feature} between {year1} and {year2}')
    plt.title(f'Top {n} Average {feature}')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()
    return result

def Company_vs_median(dataframe, feature, j_name, ont):
    org_n = ont.index[ont == j_name][0]
    company_data = dataframe[dataframe['organization number'] == int(org_n)]
    if company_data.empty:
        print(f"No data found for organization number: {org_n}")
        return
    company_value = company_data[feature].iloc[0]
    category_median_value = dataframe[dataframe['category'] == company_data['category']][feature].median()
    plt.figure(figsize=(8, 6))
    plt.bar(['Company', 'Median'], [company_value, category_median_value], color=['blue', 'gray'])
    plt.title(f"{feature} Comparison for Company vs Median")
    plt.xlabel('Category')
    plt.ylabel(feature)
    plt.show()
    
def Category_comparison(df, feature_x, feature_y, j_name, ont):
    org_n = ont.index[ont == j_name][0]
    work_df = df.dropna(subset = [feature_x, feature_y])
    target_company = work_df[work_df['organization number'] == int(org_n)]
    target_region = int(target_company.iloc[0]['region_ID'])
    target_category = int(target_company.iloc[0]['category'])
    target_companions = work_df[(work_df['region_ID'] == target_region) & (work_df['category'] == target_category)].drop(['region_ID', 'category'], axis = 1)
    median_feature_x = target_companions[feature_x].median()
    median_feature_y = target_companions[feature_y].median()
    plt.figure(figsize=(32, 10))
    for i, row in target_companions.iterrows():
        if row[feature_x] > median_feature_x and row[feature_y] > median_feature_y:
            color = 'green'
            marker = 'o'
        elif row[feature_x] < median_feature_x and row[feature_y] < median_feature_y:
            color = 'red'
            marker = 's'
        else:
            color = 'black'
        plt.scatter(row[feature_x], row[feature_y], label=ont[ont.index.get_loc(str(int(row['organization number'])))], c=color, alpha=0.7, marker=marker)
    plt.xlabel(feature_x)
    plt.ylabel(feature_y)
    mean_x, std_x = work_df[feature_x].mean(), work_df[feature_x].std()
    mean_y, std_y = work_df[feature_y].mean(), work_df[feature_y].std()
    width = 1
    plt.xlim(mean_x - width * std_x, mean_x + width * std_x)
    plt.ylim(mean_y - width * std_y, mean_y + width * std_y)
    texts = [plt.text(row[feature_x], row[feature_y], ont[ont.index.get_loc(str(int(row['organization number'])))], fontsize=10) for i, row in target_companions.iterrows()]
    adjust_text(texts, ax=plt.gca(), arrowprops=dict(arrowstyle='-', color='black'), only_move={'points': 'y', 'text': 'y', 'static': (0.8, 0.2), 'pull': (0.2, 0.2), 'explode': (1.2, 1.2)}, force_text=(0.2, 0.8))
    plt.show()

def Plot_list(companies, feature_x, feature_y, feature_size, ont, width_x, width_y):
    plt.figure(figsize=(15, 15))
    max_size = companies[feature_size].max() * 0.1
    min_size = companies[feature_size].min() * 0.1
    for _, company in companies.iterrows():
        company_name = ont[ont.index.get_loc(str(int(company['organization number'].replace('-', ''))))]
        plt.scatter(company[feature_x], company[feature_y], s=200 * (company[feature_size] - min_size) / (max_size - min_size), label=company_name, alpha=1)
    plt.xlabel(feature_x)
    plt.ylabel(feature_y)
    
    std_x, median_x = companies[feature_x].std(), companies[feature_x].median()
    std_y, median_y = companies[feature_y].std(), companies[feature_y].median()
    
    plt.xlim(median_x - width_x * std_x, median_x + width_x * std_x)
    plt.ylim(median_y - width_y * std_y, median_y + width_y * std_y)
    
    plt.axvline(x=median_x, color='black', linestyle='--')
    plt.axhline(y=median_y, color='black', linestyle='--')
    
    texts = [plt.text(company[feature_x], company[feature_y], ont.get(str(int(company['organization number'].replace('-', '')))), fontsize=12) for _, company in companies.iterrows()]
    adjust_text(texts, ax=plt.gca(), arrowprops=dict(arrowstyle='-', color='black'), only_move={'points': 'y', 'text': 'y', 'static': (0.8, 0.2), 'pull': (0.2, 0.2), 'explode': (1.2, 1.2)}, force_text=(0.2, 0.8))
    plt.show()


Activity_types_translation_table = {
    'Alkoholhaltiga drycker, Butikshandel': 'Alcoholic Beverages, Retail Trade',
    'Anläggningsarbeten': 'Construction Works',
    'Antikviteter & Beg. Böcker, Butikshandel': 'Antiques & Second-Hand Books, Retail Trade',
    'Apotekshandel': 'Pharmacy Retail Trade',
    'Arbetsförmedling & Rekrytering': 'Employment Agencies & Recruitment',
    'Arkitektverksamhet': 'Architectural Services',
    'Auktioner': 'Auctions',
    'Band & Skivor, Butikshandel': 'Music & Records, Retail Trade',
    'Barnkläder, Butikshandel': 'Children''s Clothing, Retail Trade',
    'Begagnade varor övriga, Butikshandel': 'Used Goods, Other, Retail Trade',
    'Belysningsartiklar, Butikshandel': 'Lighting Articles, Retail Trade',
    'Blommor & Växter, Butikshandel': 'Flowers & Plants, Retail Trade',
    'Bröd & Konditori, Butikshandel': 'Bread & Pastry, Retail Trade',
    'Butikshandel, övrig': 'Retail Trade, Other',
    'Byggnadssnickeriarbeten': 'Building Carpentry Works',
    'Byggverksamhet': 'Construction',
    'Båtar, Butikshandel': 'Boats, Retail Trade',
    'Böcker, Butikshandel': 'Books, Retail Trade',
    'Campingplatsverksamhet': 'Camping Site Operation',
    'Catering': 'Catering',
    'Centralkök för sjukhus': 'Central Kitchen for Hospitals',
    'Centralkök för skolor & omsorg': 'Central Kitchen for Schools & Care',
    'Cyklar, Butikshandel': 'Bicycles, Retail Trade',
    'Damkläder, Butikshandel': 'Women''s Clothing, Retail Trade',
    'Datakonsultverksamhet': 'Data Consulting Services',
    'Dataprogrammering': 'Data Programming',
    'Datordrifttjänster': 'Data Hosting Services',
    'Datorer, Program, Data-& TV-spel, Butikshandel': 'Computers, Software, Data & Video Games, Retail Trade',
    'Detaljhandel, övrig': 'Retail Trade, Other',
    'Drivmedel, Detaljhandel': 'Fuel, Retail Trade',
    'El-VVS & Bygginstallationer': 'Electrical, Plumbing & Building Installations',
    'Elektriska Hushållsmaskiner, Butikshandel': 'Electrical Household Appliances, Retail Trade',
    'Fastighetsförmedling': 'Real Estate Agency',
    'Fastighetsförvaltning på uppdrag': 'Real Estate Management on Behalf',
    'Fastighetsrelaterade stödtjänster': 'Real Estate-Related Support Services',
    'Finans, administrativa tjänster': 'Finance, Administrative Services',
    'Finansförmedling, övrig': 'Financial Intermediation, Other',
    'Finansiell leasing': 'Financial Leasing',
    'Finansiella stödtjänster, övriga': 'Other Financial Support Services',
    'Fisk & Skaldjur, Butikshandel': 'Fish & Seafood, Retail Trade',
    'Fondanknuten livförsäkring': 'Unit-Linked Life Insurance',
    'Fonder & liknande finansiella enheter, övriga': 'Funds & Similar Financial Entities, Other',
    'Fondförvaltning, övrig': 'Fund Management, Other',
    'Fotoutrustning, Butikshandel': 'Photographic Equipment, Retail Trade',
    'Frukt & Grönsaker, Butikshandel': 'Fruits & Vegetables, Retail Trade',
    'Färg & Lack, Butikshandel': 'Paint & Varnish, Retail Trade',
    'Försäkring & Pensionsfond stödtjänster, övriga': 'Insurance & Pension Fund Support Services, Other',
    'Förvaltning & Handel med Värdepapper': 'Management & Trading of Securities',
    'Förvaltning & Handel med värdepapper': 'Securities Management & Trading',
    'Förvaltning av investeringsfonder': 'Investment Fund Management',
    'Förvaltning i Bostadsrättsföreningar': 'Management in Housing Cooperatives',
    'Glas & Porslin, Butikshandel': 'Glass & Porcelain, Retail Trade',
    'Glasmästeriarbeten': 'Glass Cutting Works',
    'Golv- & Väggbeläggningsarbeten': 'Flooring & Wall Covering Works',
    'Grafisk Designverksamhet': 'Graphic Design Services',
    'Guldsmedsvaror & Smycken, Butikshandel': 'Goldsmith''s Goods & Jewelry, Retail Trade',
    'Handel med egna fastigheter': 'Trade with Own Properties',
    'Herrkläder, Butikshandel': 'Men''s Clothing, Retail Trade',
    'Holdingverksamhet i finansiella koncerner': 'Holding Company in Financial Conglomerates',
    'Holdingverksamhet i icke-finansiella koncerner': 'Holding Company in Non-Financial Conglomerates',
    'Hotell & Restaurang': 'Hotel & Restaurant',
    'Hälso- & Sjukvård, övriga': 'Healthcare, Other',
    'Hälsokost, Butikshandel': 'Health Food, Retail Trade',
    'IT- & Datatjänster, övriga': 'IT & Data Services, Other',
    'Industri- & Produktdesignverksamhet': 'Industrial & Product Design Services',
    'Inredningsarkitekt': 'Interior Architect',
    'Inredningstextilier, Butikshandel': 'Interior Textiles, Retail Trade',
    'Investeringsfonder': 'Investment Funds',
    'Investment- & Riskkapitalbolag': 'Investment & Venture Capital Companies',
    'Järn- & VVS- varor, Butikshandel': 'Iron & Plumbing Goods, Retail Trade',
    'Klockor, Butikshandel': 'Watches, Retail Trade',
    'Kläder, Butikshandel': 'Clothing, Retail Trade',
    'Konfektyr, Butikshandel': 'Confectionery, Retail Trade',
    'Konferensanläggningar': 'Conference Facilities',
    'Konst & Galleri, Butikshandel': 'Art & Gallery, Retail Trade',
    'Kontorsförbrukningsvaror, Butikshandel': 'Office Consumables, Retail Trade',
    'Kontorsmöbler, Butikshandel': 'Office Furniture, Retail Trade',
    'Kosmetika & Hygienartiklar, Butikshandel': 'Cosmetics & Hygiene Articles, Retail Trade',
    'Kreditgivning, övrig': 'Credit Provision, Other',
    'Kött & Charkuterier, Butikshandel': 'Meat & Charcuterie, Retail Trade',
    'Livförsäkring, övrig': 'Life Insurance, Other',
    'Livsmedel övriga, Butikshandel': 'Other Foodstuffs, Retail Trade',
    'Livsmedelshandel': 'Food Retail Trade',
    'Ljud & Bild, Butikshandel': 'Audio & Video, Retail Trade',
    'Logiverksamhet, övrig': 'Logistics Services, Other',
    'Markundersökning': 'Market Research',
    'Mattor & Väggbeklädnad, Butikshandel': 'Carpets & Wall Coverings, Retail Trade',
    'Monetär finansförmedling, övrig': 'Monetary Financial Intermediation, Other',
    'Musikinstrument & Noter, Butikshandel': 'Musical Instruments & Scores, Retail Trade',
    'Mynt & Frimärken, Butikshandel': 'Coins & Stamps, Retail Trade',
    'Måleriarbeten': 'Painting Works',
    'Möbler för hemmet, Butikshandel': 'Furniture for the Home, Retail Trade',
    'Optiker, Butikshandel': 'Optician, Retail Trade',
    'Pensionsfondsverksamhet': 'Pension Fund Activities',
    'Personalfunktioner, övrigt': 'Personnel Functions, Other',
    'Personalmatsalar': 'Employee Cafeterias',
    'Personaluthyrning': 'Staff Leasing',
    'Postorder- & Internethandel': 'Mail Order & Internet Trade',
    'Primärvårdsmottagning': 'Primary Care Clinic',
    'Puts-, Fasad- & Stuckatörsarbeten': 'Cleaning & Facility Management',
    'Pälsar, Butikshandel': 'Furs, Retail Trade',
    'Rengöring & Lokalvård': 'Cleaning & Facility Management',
    'Restaurangverksamhet': 'Restaurant Operations',
    'Risk- & Skadebedömning': 'Risk & Damage Assessment',
    'Rivning av hus & byggnader': 'Demolition of Houses & Buildings',
    'Sjukvårdsartiklar, Butikshandel': 'Medical Supplies, Retail Trade',
    'Skadeförsäkring': 'Insurance Claims',
    'Skor, Butikshandel': 'Shoes, Retail Trade',
    'Skötsel & Underhåll av Grönytor': 'Care & Maintenance of Green Areas',
    'Slutbehandling av byggnader': 'Finalization of Buildings',
    'Sluten Sjukvård': 'Closed Healthcare',
    'Sociala insatser': 'Social Services',
    'Spel & Leksaker, Butikshandel': 'Games & Toys, Retail Trade',
    'Sport- & Fritidsartiklar, Butikshandel': 'Sports & Leisure Articles, Retail Trade',
    'Stugbyverksamhet': 'Cottage Rentals',
    'Sällskapsdjur, Butikshandel': 'Companion Animals, Retail Trade',
    'Takarbeten': 'Roofing',
    'Teknisk konsult inom Bygg- & Anläggningsteknik': 'Technical Consultancy in Building & Construction Engineering',
    'Telekommunikation, Satellit': 'Telecommunication, Satellite',
    'Telekommunikation, Trådbunden': 'Telecommunication, Wired',
    'Telekommunikation, Trådlös': 'Telecommunication, Wireless',
    'Telekommunikation, övrig': 'Telecommunication, Other',
    'Telekommunikationsutrustning, Butikshandel': 'Telecommunications Equipment, Retail Trade',
    'Textilier, Butikshandel': 'Textiles, Retail Trade',
    'Tidningar, Butikshandel': 'Newspapers, Retail Trade',
    'Tobaksvaror, Butikshandel': 'Tobacco Products, Retail Trade',
    'Torg- & Marknadshandel': 'Square & Market Trade',
    'Utformning av Byggprojekt': 'Design of Building Projects',
    'Utgivning av annan programvara': 'Publication of Other Software',
    'Utgivning av dataspel': 'Publication of Video Games',
    'Uthyrning & Förvaltning av Fastigheter': 'Rental & Management of Properties',
    'Uthyrning av Bygg- & Anläggningsmaskiner med förare': 'Rental of Building & Construction Machinery with Operator',
    'Vandrarhemsverksamhet': 'Hostel Operations',
    'Varuhus- & Stormarknadshandel': 'Department Store & Hypermarket Trade',
    'Verksamhet utförd av Försäkringsombud & Försäkringsmäklare': 'Activities carried out by Insurance Agents & Brokers',
    'Verksamhet utförd av Värdepappers- & Varumäklare': 'Activities carried out by Securities & Commodities Brokers',
    'Virke & Byggvaror, Butikshandel': 'Timber & Building Materials, Retail Trade',
    'Väskor & Reseffekter, Butikshandel': 'Bags & Travel Items, Retail Trade',
    'Vård & Omsorg med Boende': 'Healthcare & Care with Accommodation',
    'Återförsäkring': 'Reinsurance',
    'Bränsle, Mineraler & Industrikem. Partihandel': 'Fuel, Minerals & Industrial Chemicals Wholesale',
    'Datorer, Program & Kringutr, Partihandel': 'Computers, Software & Peripherals Wholesale',
    'Drivning': 'Driving',
    'Elektronikindustri': 'Electronics Industry',
    'Elektronikkomponenter, Partihandel': 'Electronic Components Wholesale',
    'Elgenerering': 'Electric Generation',
    'Farmaceutiska basprodukter, tillverkning': 'Pharmaceutical Raw Materials Manufacturing',
    'Gruv-, Bygg- & Anläggningsmaskiner, Partihandel': 'Mining, Construction & Construction Machinery Wholesale',
    'Hushållsapparater & Elartiklar, Partihandel': 'Household Appliances & Electrical Articles Wholesale',
    'Insatsvaror övriga, Partihandel': 'Other Input Goods, Wholesale',
    'Järnhandelsvaror, Partihandel': 'Ironmongery Wholesale',
    'Kemiska produkter, tillverkning': 'Chemical Products Manufacturing',
    'Konsultverksamhet avseende företags org.': 'Consultancy on Corporate Organization',
    'Kontors- & Butiksinred, tillverkning': 'Office & Shop Fittings Manufacturing',
    'Ljud, Bild & Videoutrustning, Partihandel': 'Audio, Video & Visual Equipment Wholesale',
    'Läkemedel, tillverkning': 'Pharmaceutical Manufacturing',
    'Metallindustri': 'Metal Industry',
    'Pappers- & Pappersvarutillverkning': 'Paper & Paper Product Manufacturing',
    'Partihandel, övrig': 'Wholesale Trade, Other',
    'Personbilar & Lätta Motorfordon, Handel': 'Cars & Light Motor Vehicles, Retail Trade',
    'Rälsfordon, tillverkning': 'Rail Vehicles Manufacturing',
    'Skogsförvaltning': 'Forest Management',
    'Specialistläkare ej på sjukhus': 'Specialist Doctors not in Hospitals',
    'Specialistläkare på sjukhus': 'Specialist Doctors in Hospitals',
    'Spel- & Vadhållningsverksamhet': 'Gambling & Betting',
    'Stats- & Kommunledning, Lagstiftning & Planering': 'State & Municipal Management, Legislation & Planning',
    'Sågning, Hyvling & Impregnering': 'Sawing, Planing & Impregnation',
    'Tandläkare': 'Dentists',
    'Transport stödtjänster, övriga': 'Transport Support Services, Other',
    'Transportmedelsindustri': 'Transport Industry',
    'Verksamheter som utövas av huvudkontor': 'Activities of Head Offices',
    'Veterinärverksamhet': 'Veterinary Activities',
    'Virke & Byggmaterial, Partihandel': 'Timber & Building Materials, Wholesale',
    'Värme & Kyla': 'Heat & Cold',
    'Öppen Hälso- & Sjukvård': 'Open Health & Medical Care',
    'Kemiska produkter, Partihandel': 'Chemical Products, Wholesale',
    'Juridik, Ekonomi, Vetenskap & Teknik, övrig': 'Law, Economics',
    'nan': 'Nanotechnology',
    'Vägtransport, Godstrafik': 'Road Transport, Freight Traffic',
    'Verktygsmaskiner, tillverkning': 'Machine Tools Manufacturing',
    'Vatten & Avlopp': 'Water & Sewerage',
    'VVS-varor, Partihandel': 'HVAC Goods, Wholesale',
    'Uthyrning & Leasing av Bygg- & Anläggningsmaskiner': 'Rental & Leasing of Building & Construction Machinery',
    'Trävaror, tillverkning': 'Wood Products Manufacturing',
    'Trähus, tillverkning': 'Wooden Houses Manufacturing',
    'Teknisk konsult inom Industriteknik': 'Technical Consultancy in Industrial Engineering',
    'Teknisk konsult inom Elteknik': 'Technical Consultancy in Electrical Engineering',
    'Taxi': 'Taxi',
    'Plastförpackningstillverkning': 'Plastic Packaging Manufacturing',
    'Möbler, Mattor & Belysning, Partihandel': 'Furniture, Carpets & Lighting',
    'Motorcyklar, Handel, service & tillbehör': 'Motorcycles, Trade',
    'Maskiner, tillverkning': 'Machinery Manufacturing',
    'Maskiner, reparation': 'Machinery Repair',
    'Maskiner & Utrustning övriga, Partihandel': 'Other Machinery & Equipment, Wholesale',
    'Lastbilar, Bussar & Specialfordon, Handel': 'Trucks, Buses & Special Vehicles',
    'Köttprodukter': 'Meat Products',
    'Kläder & Skor, Partihandel': 'Clothing & Shoes, Wholesale',
    'Juridisk verksamhet, övrig': 'Legal Activities, Other',
    'Jord- & Skogsbruksmaskiner, tillverkning': 'Agricultural & Forestry Machinery Manufacturing',
    'Husvagnar, Husbilar & Släp, Handel': 'Caravans, Motorhomes & Trailers, Retail Trade',
    'Gummivaror, tillverkning': 'Rubber Goods Manufacturing',
    'Glasstillverkning': 'Glass Manufacturing',
    'Elektriska Komponenter & Kretskort, tillverkning': 'Electrical Components & Circuit Boards Manufacturing',
    'Elartiklar, Partihandel': 'Electrical Articles, Wholesale',
    'Elapparatur, Reparation': 'Electrical Equipment, Repair',
    'Byggnadsmetallvaror, tillverkning': 'Building Metal Goods Manufacturing',
    'Yrkesförarutbildning': 'Professional Driver Training',
    'Verktyg & Redskap, tillverkning': 'Tools & Implements Manufacturing',
    'Tryckning av Böcker & Övrigt': 'Book Printing & Other',
    'Teknisk konsult inom Energi-, Miljö- & VVS-teknik': 'Technical Consultancy in Energy, Environmental & HVAC Engineering',
    'Teknisk Provning & Analys': 'Technical Testing & Analysis',
    'Säkerhetssystemtjänster': 'Security System Services',
    'Stenhuggning': 'Stone Cutting',
    'Skogsskötsel': 'Forestry Management',
    'Skogsbruk': 'Forestry',
    'Sjötransport, stödtjänster': 'Maritime Transport Support Services',
    'Reklam, PR, Mediebyrå & Annonsförsälj.': 'Advertising, PR, Media Agency & Ad Sales',
    'Redovisning & bokföring': 'Accounting & Bookkeeping',
    'Plastvarutillverkning, övrig': 'Plastic Goods Manufacturing, Other',
    'Plastvaror, tillverkning': 'Plastic Goods Manufacturing',
    'Personalutbildning': 'Staff Training',
    'Mät- & Precisionsinstrument, Partihandel': 'Measurement & Precision Instruments, Wholesale',
    'Motorfordon, reparation & underhåll': 'Motor Vehicles, Repair & Maintenance',
    'Mjölkproduktion & Nötkreatursuppfödning': 'Milk Production & Cattle Farming',
    'Mineralutvinning, övrig': 'Mineral Extraction, Other',
    'Marknads- & Opinionsundersökning': 'Market & Opinion Research',
    'Livsmedelsframställning': 'Food Production',
    'Landtransport, stödtjänster': 'Land Transport Support Services',
    'Landtransport av passagerare, övrig': 'Passenger Land Transport, Other',
    'Källsorterat material': 'Source-Sorted Material',
    'Kroppsvård': 'Personal Care',
    'Jordbruksmaskiner, Partihandel': 'Agricultural Machinery, Wholesale',
    'Industri- Maskiner & Utrustning, installation': 'Industrial Machinery & Equipment, Installation',
    'Frukt & Grönsaker, Partihandel': 'Fruits & Vegetables, Wholesale',
    'Flyttjänster': 'Moving Services',
    'Fiskodling': 'Fish Farming',
    'Fisk, Skalddjur & andra livsmedel, Partihandel': 'Fish, Shellfish & Other Food, Wholesale',
    'Dörrar av Trä, tillverkning': 'Wooden Doors Manufacturing',
    'Databehandling & Hosting': 'Data Processing & Hosting',
    'Båt & Fartyg, tillverkning': 'Boat & Ship Manufacturing',
    'Byggplastvarutillverkning': 'Building Plastic Goods Manufacturing',
    'Webbportaler': 'Web Portals',
    'Verktygsmaskiner, Partihandel': 'Machine Tools, Wholesale',
    'Utbildning, övrig': 'Education, Other',
    'Transportmedel övriga, tillverkning': 'Other Transport Equipment, Manufacturing',
    'Textilier, Kläder & Skodon, Partihandel': 'Textiles, Clothing & Footwear, Wholesale',
    'Teknisk konsultverksamhet, övrig': 'Technical Consultancy, Other',
    'Sportverksamhet, övrig': 'Sports Activities, Other',
    'Sport- & Fritidsartiklar, Partihandel': 'Sports & Leisure Articles, Wholesale',
    'Specialsortiment': 'Special Assortment',
    'Smyckestillverkning': 'Jewelry Manufacturing',
    'Skidsportanläggningar': 'Ski Resorts',
    'Service till husdjursskötsel': 'Pet Care Services',
    'Service till Skogsbruk': 'Forestry Services',
    'Plantskoleväxter, odling': 'Nursery Plants, Cultivation',
    'Möbler övriga, tillverkning': 'Other Furniture Manufacturing',
    'Motorfordon, reservdelar & tillbehör': 'Motor Vehicles, Parts & Accessories',
    'Medicinsk utrustning & Apoteksvaror, Partihandel': 'Medical Equipment & Pharmacy Goods, Wholesale',
    'Litterärt & Konstnärligt skapande': 'Literary & Artistic Creation',
    'Kött & Köttvaror, Partihandel': 'Meat & Meat Products, Wholesale',
    'Köksmöbler, tillverkning': 'Kitchen Furniture Manufacturing',
    'Kultur, Nöje & Fritid': 'Culture, Entertainment & Leisure',
    'Kontorsförbrukningsvaror, Partihandel': 'Office Supplies, Wholesale',
    'Kläder & Textilier, tillverkning': 'Clothing & Textiles, Manufacturing',
    'Järnmalmsutvinning': 'Iron Ore Extraction',
    'Industriförnödenheter, Partihandel': 'Industrial Supplies, Wholesale',
    'Idrottsplatser & Sportanläggningar': 'Sports Facilities & Sports Centers',
    'Hushållsvaror övriga, Partihandel': 'Household Goods, Other, Wholesale',
    'Hushålls- & Personartiklar, reparation, övriga': 'Household & Personal Items, Repair, Other',
    'Frisörer': 'Hairdressers',
    'Callcenterverksamhet': 'Call Center Operations',
    'Bioteknisk Forskning & Utveckling': 'Biotechnical Research & Development',
    'Betongvarutillverkning': 'Concrete Goods Manufacturing',
    'Avfallshantering & Återvinning': 'Waste Management & Recycling',
    'Artistisk verksamhet': 'Artistic Activities',
    'Advokatbyråer': 'Law Firms',
    'Översättning & Tolkning': 'Translation & Interpretation',
    'Äggproduktion': 'Egg Production',
    'Vård av historiska Minnesmärken & Byggnader': 'Care of Historical Monuments & Buildings',
    'Växtodling': 'Crop Farming',
    'Växter, bearbetning': 'Plants, Processing',
    'Vävnadstillverkning': 'Tissue Manufacturing',
    'Virkesmätning': 'Timber Measurement',
    'Vapen & Ammunition, tillverkning': 'Weapons & Ammunition, Manufacturing',
    'Utvinning, stödtjänster': 'Extraction, Support Services',
    'Uttjänta fordon, Partihandel': 'Used Vehicles, Wholesale',
    'Utrustning Reparation, övrig': 'Equipment Repair, Other',
    'Utrikesförvaltning': 'Foreign Affairs',
    'Uthyrning av Videokassetter & Dvd-skivor': 'Rental of Videotapes & DVD Discs',
    'Uthyrning & Leasing övrigt': 'Rental & Leasing, Other',
    'Uthyrning & Leasing av flygplan': 'Rental & Leasing of Aircraft',
    'Uthyrning & Leasing av andra Hushållsartiklar & Varor för Personligt bruk': 'Rental & Leasing of Other Household Items & Personal Use Goods',
    'Uthyrning & Leasing av Personbilar & lätta Motorfordon': 'Rental & Leasing of Cars & Light Motor Vehicles',
    'Uthyrning & Leasing av Lastbilar & andra tunga Motorfordon': 'Rental & Leasing of Trucks & Other Heavy Motor Vehicles',
    'Uthyrning & Leasing av Kontorsmaskiner & Kontorsutrustning (inklusive datorer)': 'Rental & Leasing of Office Machinery & Equipment (including computers)',
    'Uthyrning & Leasing av Jordbruksmaskiner & Jordbruksredskap': 'Rental & Leasing of Agricultural Machinery & Equipment',
    'Uthyrning & Leasing av Fritids- & Sportutrustning': 'Rental & Leasing of Recreational & Sports Equipment',
    'Uthyrning & Leasing av Fartyg & Båtar': 'Rental & Leasing of Ships & Boats',
    'Utgivning av tidskrifter': 'Magazine Publishing',
    'Utbildningsväsendet, stödverksamhet': 'Education, Support Activities',
    'Urtillverkning': 'Clock Manufacturing',
    'Uran- & Toriummalmutvinning': 'Uranium & Thorium Ore Mining',
    'Ur & Guldssmedsvaror, Partihandel': 'Clocks & Goldsmith Goods, Wholesale',
    'Ur & Guldsmedsvaror, reparation': 'Clocks & Goldsmith Goods, Repair',
    'Universitets- & Högskoleutbildning samt Forskning': 'University & Higher Education and Research',
    'Universitets- & Högskoleutbildning': 'University & Higher Education',
    'Tävling med hästar': 'Horse Racing',
    'Tvål, Såpa & Tvättmedel, tillverkning': 'Soap & Detergent Manufacturing',
    'Tvätteriverksamhet': 'Laundry Operations',
    'Turist- & Bokningsservice': 'Tourist & Booking Services',
    'Trädgårdar, Djurparker & Naturreservat, drift': 'Gardens, Zoos & Nature Reserves, Operation',
    'Tryckning av Tidsskrifter': 'Printing of Journals',
    'Tryckning av Dagstidningar': 'Printing of Newspapers',
    'Trav- & Galoppbanor': 'Trotting & Galloping Tracks',
    'Transportmedel övriga, reparation': 'Other Transport Equipment, Repair',
    'Trafikskoleverksamhet': 'Driving School Operations',
    'Torvutvinning': 'Peat Extraction',
    'Tobaksvarutillverkning': 'Tobacco Product Manufacturing',
    'Tobaksodling': 'Tobacco Cultivation',
    'Tobak, Partihandel': 'Tobacco, Wholesale',
    'Tillverkning, övrig': 'Manufacturing, Other',
    'Textilier, Partihandel': 'Textiles, Wholesale',
    'Textil-, Sy- & Stickmaskiner, Partihandel': 'Textile, Sewing & Knitting Machines, Wholesale',
    'Teleprodukter, Partihandel': 'Telecom Products, Wholesale',
    'Te- & Kaffetillverkning': 'Tea & Coffee Production',
    'Tandprotestillverkning': 'Denture Production',
    'TV-program planering': 'TV Program Planning',
    'Sötvattensfiske': 'Freshwater Fishing',
    'Sällskapsdjur, uppfödning': 'Companion Animal Breeding',
    'Säkerhetsverksamhet': 'Security Operations',
    'Syntetiskt basgummi, tillverkning': 'Synthetic Base Rubber Manufacturing',
    'Svampodling': 'Mushroom Cultivation',
    'Stärkelsetillverkning': 'Starch Production',
    'Studieförbundens & Frivilligorg. utbildning': 'Study Associations & Voluntary Organization Education',
    'Strumpor, tillverkning': 'Socks Manufacturing',
    'Sprängämne, tillverkning': 'Explosive Manufacturing',
    'Sportklubbars & Idrottsför.  verksamhet': 'Sports Clubs & Athletic Association Activities',
    'Sportartikelstillverkning': 'Sports Article Manufacturing',
    'Sport- & Fritidsutbildning': 'Sports & Leisure Education',
    'Spel- & Leksakstillverkning': 'Game & Toy Manufacturing',
    'Spannmål, Balj- & Oljeväxter, odling': 'Cereal, Legume & Oilseed Cultivation',
    'Socketbetsodling': 'Safflower Cultivation',
    'Sockertillverkning': 'Sugar Manufacturing',
    'Socker, Choklad & Sockerkonfekt, Partihandel': 'Sugar, Chocolate & Confectionery, Wholesale',
    'Smågrisuppfödning': 'Piglet Farming',
    'Slaktsvinuppfödning': 'Pork Production',
    'Skönhetsvård': 'Beauty Care',
    'Skor, tillverkning': 'Shoe Manufacturing',
    'Skomakare': 'Cobbler',
    'Skatterådgivning': 'Tax Consultancy',
    'Service till växtodling': 'Crop Service',
    'Sanitetsgods, Partihandel': 'Sanitary Goods, Wholesale',
    'Samhällsvet. & Humanistisk F&U': 'Social Sciences & Humanities R&D',
    'Samhällelig informationsförsörjning': 'Social Information Supply',
    'Saltvattensfiske': 'Saltwater Fishing',
    'Råpetroleumutvinning': 'Crude Petroleum Extraction',
    'Revision': 'Audit',
    'Resebyråer': 'Travel Agencies',
    'Researrangemang': 'Travel Arrangements',
    'Reproduktion av inspelningar': 'Reproduction of Recordings',
    'Renskötsel': 'Reindeer Husbandry',
    'Religiösa samfund': 'Religious Communities',
    'Reklamfotoverksamhet': 'Advertising Photography',
    'Radiosändning': 'Radio Broadcasting',
    'Pälsvaror, tillverkning': 'Fur Goods Manufacturing',
    'Publicering av Kataloger & Sändlistor': 'Publication of Catalogs & Broadcasting Schedules',
    'Press- & Övrig Fotografverksamhet': 'Press & Other Photography Activities',
    'Potatisodling': 'Potato Cultivation',
    'Potatisberedning': 'Potato Processing',
    'Porträttfotoverksamhet': 'Portrait Photography',
    'Planglas': 'Flat Glass',
    'Petroleumraffinering': 'Petroleum Refining',
    'Personalförvaltning & andra stödtjänster': 'Personnel Management & Other Support Services',
    'Patentbyråer': 'Patent Agencies',
    'Partihandel': 'Wholesale Trade',
    'Parfym & Kosmetika, Partihandel': 'Perfume & Cosmetics, Wholesale',
    'Organiska baskemikalier, tillverkning': 'Organic Basic Chemicals Manufacturing',
    'Optiska instrument & Fotoutrustning': 'Optical Instruments & Photographic Equipment',
    'Optiska fiberkabeltillverkning': 'Optical Fiber Cable Manufacturing',
    'Oorganiska baskemikalier, tillverkning': 'Inorganic Basic Chemicals Manufacturing',
    'Omsorg & Socialtjänst': 'Care & Social Services',
    'Nötkreatur & Bufflar, övriga': 'Other Cattle & Buffaloes',
    'Nöjes- & Temaparksverksamhet': 'Amusement & Theme Park Activities',
    'Näringslivsprogram, övriga': 'Business Programs, Other',
    'Nyhetsservice': 'News Service',
    'Naturvetenskaplig och Teknisk F&U': 'Natural Science and Technical R&D',
    'Möbler, Hushålls- & Järnhandelsvaror, Partihandel': 'Furniture, Household & Ironmongery, Wholesale',
    'Möbler & Heminredning, reparation': 'Furniture & Home Furnishing, Repair',
    'Musikinstrumenttillverkning': 'Musical Instrument Manufacturing',
    'Musik-, Dans- & Kulturell utbildning': 'Music, Dance & Cultural Education',
    'Museiverksamhet': 'Museum Activities',
    'Murbrukstillverkning': 'Mortar Manufacturing',
    'Motorfordonstillverkning': 'Motor Vehicle Manufacturing',
    'Motorer & Turbiner, tillverkning': 'Engines & Turbines, Manufacturing',
    'Motorcyklar, tillverkning': 'Motorcycle Manufacturing',
    'Motorbanor': 'Motor Racing Tracks',
    'Militärt försvar': 'Military Defense',
    'Militära stridsfordon, tillverkning': 'Military Armored Vehicle Manufacturing',
    'Metallvaror, reparation': 'Metal Goods, Repair',
    'Metaller & Metallmalmer, Partihandel': 'Metals & Metal Ores, Wholesale',
    'Metallavfall & Metallskrot, Partihandel': 'Metal Scrap & Metal Waste, Wholesale',
    'Mejerivarutillverkning': 'Dairy Product Manufacturing',
    'Mejeriprodukter, Ägg, Matolja & Matfett, Partihandel': 'Dairy Products, Eggs, Edible Oils & Fats,',
    'Medicin- & Dentalinstrumenttillverkning': 'Medical & Dental Instrument Manufacturing',
    'Mattor, tillverkning': 'Carpet Manufacturing',
    'Maskiner, Industriell utrustning, Partihandel': 'Machinery, Industrial Equipment, Wholesale',
    'Malmutvinning, övrig': 'Ore Mining, Other',
    'Magasinering & Varulagring': 'Warehousing & Storage',
    'Madrasser, tillverkning': 'Mattress Manufacturing',
    'Läder- & Skinnkläder, tillverkning': 'Leather & Fur Clothing Manufacturing',
    'Lufttransporter, stödtjänster': 'Air Transport Support Services',
    'Lufttransport, Passagerartrafik': 'Air Transport, Passenger Traffic',
    'Lufttransport, Godstrafik': 'Air Transport, Freight Traffic',
    'Luftfartyg & Rymdfarkoster, Reparation': 'Aircraft & Spacecraft, Repair',
    'Ljudinspelning & fonogramutgivning': 'Sound Recording & Phonogram Publishing',
    'Livsmedel, Dryck & Tobak,  Partihandel': 'Food, Beverage & Tobacco, Wholesale',
    'Linjebussverksamhet': 'Bus Transport Operations',
    'Lim, tillverkning': 'Adhesive Manufacturing',
    'Levande Djur, Partihandel': 'Live Animals, Wholesale',
    'Leasing av immateriell egendom & liknande prod.': 'Leasing of Intellectual Property & Similar Products',
    'Köksinredningar, tillverkning': 'Kitchen Furniture Manufacturing',
    'Kvarnprodukter': 'Mill Products',
    'Kultur, Miljö, Boende, administration': 'Culture, Environment, Accommodation,',
    'Kontorsutrustning & Datorer, Partihandel': 'Office Equipment & Computers, Wholesale',
    'Kontorstjänster': 'Office Services',
    'Kontorsmöbler, Partihandel': 'Office Furniture, Wholesale',
    'Kontorsmaskiner, tillverkning': 'Office Machine Manufacturing',
    'Kontorsmaskiner & Kontorsutrustning, Partihandel': 'Office Machines & Equipment, Wholesale',
    'Kontors- & Butiksmöber, tillverkning': 'Office & Shop Furniture, Manufacturing',
    'Konsumenttjänster, övriga': 'Consumer Services, Other',
    'Konstfiber, tillverkning': 'Artificial Fiber Manufacturing',
    'Kongresser & Mässor': 'Congresses & Fairs',
    'Kommunikationsutrustning, tillverkning': 'Communication Equipment Manufacturing',
    'Kommunikationsutrustning, reparation': 'Communication Equipment Repair',
    'Kollektivtrafik, övrig': 'Public Transport, Other',
    'Keramiska produkter, tillverkning': 'Ceramic Product Manufacturing',
    'Keramiska Golv- & Väggplattor': 'Ceramic Floor & Wall Tiles',
    'Kamel- & Kameldjursuppfödning': 'Camel & Camelid Farming',
    'Kalk & Gipstillverkning': 'Lime & Plaster Manufacturing',
    'Kaffe, Te, Kakao & Kryddor, Partihandel': 'Coffee, Tea, Cocoa & Spice,',
    'Kabeltillbehör, tillverkning': 'Cable Accessories Manufacturing',
    'Järnvägstransport-Godstrafik': 'Rail Transport-Freight Traffic',
    'Järnvägstransport- Passagerartrafik': 'Rail Transport-Passenger Traffic',
    'Juice & Safttillverkning': 'Juice & Nectar Production',
    'Jordbruks- & Textilråvaror, Provisionshandel': 'Agricultural & Textile Raw Materials, Wholesale',
    'Jord- & Skogsbruk, administration av program': 'Agriculture & Forestry, Program Administration',
    'Jakt': 'Hunting',
    'Intressorganisationer, övriga': 'Interest Organizations, Other',
    'Intressebev. Yrkesorg.': 'Interest Representation Prof. Org.',
    'Intressebev. Branschorg.': 'Interest Representation Industry Org.',
    'Intressebev. Arbetstagarorg.': 'Interest Representation Labor Org.',
    'Intressebev. Arbetsgivarorg.': 'Interest Representation Employer Org.',
    'Inspektion, Kontroll & Tillståndsgivning': 'Inspection, Control & Permitting',
    'Infrastrukturprogram': 'Infrastructure Programs',
    'Industrigasframställning': 'Industrial Gas Production',
    'Icke-farligt avfall': 'Non-Hazardous Waste',
    'Icke spec. handel med livsmedel, Partihandel': 'Non-Specific Food Trade, Wholesale',
    'Hästuppfödning': 'Horse Breeding',
    'Hälso- & Sjukvård, administration': 'Healthcare Administration',
    'Hushållsapparater, Hem & Trädgård, reparation': 'Household Appliances, Home & Garden, Repair',
    'Hudar, Skinn & Läder, Partihandel': 'Hides, Skins & Leather, Wholesale',
    'Hemelektronik, tillverkning': 'Consumer Electronics, Manufacturing',
    'Hemelektronik, reparation': 'Consumer Electronics, Repair',
    'Havs- & Sjöfart, Passagerartrafik': 'Maritime & Shipping, Passenger Traffic',
    'Havs- & Sjöfart, Godstrafik': 'Maritime & Shipping, Freight Traffic',
    'Hamngodshantering': 'Port Cargo Handling',
    'Gödsel- & Kväveprodukter, tillverkning': 'Fertilizer & Nitrogen Product Manufacturing',
    'Gymnasial utbildning': 'Upper Secondary Education',
    'Gymanläggningar': 'Gymnasiums',
    'Grönsaker, växthusodling': 'Vegetable Greenhouse Cultivation',
    'Grönsaker, frilandsodling': 'Vegetables, Open-Air Cultivation',
    'Grundskoleutbildning': 'Elementary Education',
    'Grundskole- & Gymnasieskoleutbildning': 'Elementary & Secondary School Education',
    'Grafisk produktion': 'Graphic Production',
    'Golfbanor': 'Golf Courses',
    'Godshantering, övrig': 'Cargo Handling, Other',
    'Glasfibertillverkning': 'Fiberglass Manufacturing',
    'Glas- & Glasvarutillverkning': 'Glass & Glassware Manufacturing',
    'Glas, Porslin & Rengöringsmedel, Partihandel': 'Glass, Porcelain & Cleaning Products, Wholesale',
    'Gjutning': 'Casting',
    'Gipsvarutillverkning': 'Gypsum Product Manufacturing',
    'Gashandel': 'Gas Trade',
    'Gasframställning': 'Gas Production',
    'Gasdistribution': 'Gas Distribution',
    'Garntillverkning': 'Yarn Manufacturing',
    'Förvärvsarbete i hushåll': 'Domestic Work',
    'Förskoleutbildning': 'Preschool Education',
    'Förlagsverksamhet, övrig': 'Publishing Activities, Other',
    'Företagstjänster, övriga': 'Business Services, Other',
    'Fönster av Trä, tillverkning': 'Wooden Window Manufacturing',
    'Får- & Getuppfödning': 'Sheep & Goat Farming',
    'Färgämnen, tillverkning': 'Dye Manufacturing',
    'Färg, Lack & Tryckfärg, tillverkning': 'Paint, Varnish & Printing Ink Manufacturing',
    'Fruktodling': 'Fruit Cultivation',
    'Frukt, Bär & Nötter, odling': 'Fruits, Berries & Nuts, Growing',
    'Fritids-& Nöjesverksamhet, övrig': 'Leisure & Entertainment Activities, Other',
    'Fotografiska & Optiska produkter, Partihandel': 'Photographic & Optical Products, Wholesale',
    'Folkhögskoleutbildning': 'Folk High School Education',
    'Fjäderfä, uppfödning': 'Poultry Farming',
    'Fisk, Skaldjur & Blötdjur, beredning': 'Fish, Seafood & Mollusk, Processing',
    'Filmvisning': 'Film Screening',
    'Film, Video & TV': 'Film, Video & TV',
    'Fiberväxtodling': 'Fiber Crop Cultivation',
    'Fibercementvarutillverkning': 'Fiber Cement Goods Manufacturing',
    'Fartyg & Båtar, Reparation': 'Ship & Boat Repair',
    'Farligt avfall': 'Hazardous Waste',
    'Fabriksblandad Betongtillverkning': 'Ready-Mixed Concrete Manufacturing',
    'Eteriska oljor, tillverkning': 'Essential Oils Manufacturing',
    'Emballage, Partihandel': 'Packaging, Wholesale',
    'Elöverföring': 'Electric Power Transmission',
    'Elhandel': 'Electricity Trading',
    'Elektronisk & Optisk utrustning, reparation': 'Electronic & Optical Equipment Repair',
    'Elektriska Hushållsmaskiner, tillverkning': 'Manufacture of Electric Household Appliances',
    'Eldistribution': 'Electric Distribution',
    'Eldfasta produkter': 'Refractory Products',
    'Elapparatur övrig, tillverkning': 'Other Electrical Equipment Manufacturing',
    'Eftergymnasial utbildning': 'Post-Secondary Education',
    'Dryckesframställning': 'Beverage Production',
    'Drycker, Partihandel': 'Beverages, Wholesale',
    'Domstolsverksamhet': 'Court Activities',
    'Djuruppfödning, övrig': 'Other Animal Farming',
    'Djurfoderframställning': 'Animal Feed Production',
    'Direktreklamverksamhet': 'Direct Mail Advertising',
    'Datoriserad materialhanteringsutr, Partihandel': 'Computerized Material Handling Equipment, Wholesale',
    'Datorer & Kringutrustning, tillverkning': 'Computers & Peripheral Equipment Manufacturing',
    'Datorer & Kringutrustning, reparation': 'Computers & Peripheral Equipment Repair',
    'Dagstidningsutgivning': 'Newspaper Publishing',
    'Cyklar & Invalidfordon, tillverkning': 'Bicycles & Invalid Vehicles Manufacturing',
    'Civilt försvar & Frivilligförsvar': 'Civil Defense & Voluntary Defense',
    'Choklad- & Konfektyrtillverkning': 'Chocolate & Confectionery Manufacturing',
    'Cementtillverkning': 'Cement Manufacturing',
    'Bärgning': 'Salvaging',
    'Byggmaterialtillverkning': 'Building Material Manufacturing',
    'Bud- & Kurirverksamhet': 'Courier & Messenger Activities',
    'Brand- & Räddningsverksamhet': 'Firefighting & Rescue Activities',
    'Borstbinderitillverkning': 'Brush and Broom Manufacturing',
    'Bokutgivning': 'Book Publishing',
    'Blommor & Växter, Partihandel': 'Flowers & Plants, Wholesale',
    'Blandat sortiment': 'Mixed Assortment',
    'Blandat jordbruk': 'Mixed Farming',
    'Bijouteritillverkning': 'Costume Jewelry Manufacturing',
    'Bibliotek': 'Library',
    'Betong-, Cement- & Gipsvaror, övriga': 'Concrete, Cement & Plaster Products, Other',
    'Belysningsarmaturtillverkning': 'Lighting Fixture Manufacturing',
    'Bekämpningsmedel & lantbrukskem, tillverkning': 'Pesticides & Agricultural Chemicals Manufacturing',
    'Begravningsverksamhet': 'Funeral Activities',
    'Batteri- och ackumulatortillverkning': 'Battery and Accumulator Manufacturing',
    'Basplast, tillverkning': 'Basic Plastic Manufacturing',
    'Band & Skivor, Partihandel': 'Tapes & Discs, Wholesale',
    'Bageri- & Mjölprodukter': 'Bakery & Flour Products',
    'Avloppsrening': 'Wastewater Treatment',
    'Avfall & Skrot, Partihandel': 'Waste & Scrap, Wholesale',
    'Arkivverksamhet': 'Archiving',
    'Arbetsmarknadsutbildning': 'Labor Market Training',
    'Arbets- & Skyddskläder, tillverkning': 'Workwear & Protective Clothing Manufacturing',
    'Annonstidningsutgivning': 'Free Newspaper Publishing'
            }


Data = pd.read_csv('Clean_data.csv', encoding = 'utf-16')
# Data = pd.read_csv("ML_data 21x9 regions.csv")

organization_number_translation = pd.read_csv("Swedish company organization numbers.csv").drop('Unnamed: 0', axis=1)
organization_number_translation['organization number'] = organization_number_translation['organization number'].str.replace("-", "")
organization_number_translation = organization_number_translation.set_index('organization number')['juridical name']

def imbianchini_sfigati(df):
    median_quick_ratio = df['Quick ratio_2022'].median()
    median_solvency = df['Solvency_2022'].median()
    plt.figure(figsize=(12, 8))
    # If certain conditions are verified, the point is marked with a red dot; a blue square otherwise
    for i, row in df.iterrows():
        if row['Quick ratio_2022'] > median_quick_ratio and row['Solvency_2022'] > median_solvency:
            color = 'green'
            marker = 'o'
        elif row['Quick ratio_2022'] < median_quick_ratio and row['Solvency_2022'] < median_solvency:
            color = 'red'
            marker = 's'
        else:
            color = 'black'
        plt.scatter(row['Solvency_2022'], row['Quick ratio_2022'], label=row['juridical name'], c=color, alpha=0.7, marker=marker)
    plt.xlabel('Solvency_2022')
    plt.ylabel('Quick ratio_2022')
    plt.xlim(-25, 100)
    plt.ylim(0, 800)
    # Adjust_text to handle overlapping labels
    texts = [plt.text(row['Solvency_2022'], row['Quick ratio_2022'], row['juridical name'], fontsize=6) for i, row in df.iterrows()]
    adjust_text(texts)
    plt.show()

def Similar(df, region, category, n_employees):
    # Returns a subdataset of companies which belong the same region, the same category and, if specified by a positive number, the same number of employees
    if n_employees > 0 and category >= 0:
        return df[(df['region_ID'] == region) & (df['category'] == category) & (df['Number of employees_2022'] == n_employees)]
    elif n_employees < 0 and category >= 0:
        return df[(df['region_ID'] == region) & (df['category'] == category)]
    elif n_employees > 0 and category < 0:
        return df[(df['region_ID'] == region) & (df['Number of employees_2022'] == n_employees)]
    else:
        return df[(df['region_ID'] == region)]




# MAIN

Uppsala_paintworks = Data[(Data['location'] == 'Uppsala') & (Data['category'] == 'Painting Works')][['organization number', 'juridical name', 'Quick ratio_2022', 'Solvency_2022', "Number of employees_2022"]].dropna()
Plot_list(Uppsala_paintworks.head(15), "Solvency_2022", "Quick ratio_2022", "Number of employees_2022", organization_number_translation, 1, 0.33)