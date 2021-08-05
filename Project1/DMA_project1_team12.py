import mysql.connector

# TODO: REPLACE THE VALUE OF VARIABLE team (EX. TEAM 1 --> team = 1)
team = 12


# Requirement1: create schema ( name: DMA_team## )
def requirement1(host, user, password):
    cnx = mysql.connector.connect(host=host, user=user, password=password)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')

    # TODO: WRITE CODE HERE
    cursor.execute('CREATE DATABASE IF NOT EXISTS DMA_team12;')
    cursor.execute('USE DMA_team12;')

    # TODO: WRITE CODE HERE
    cursor.close()


# Requierement2: create table
def requirement2(host, user, password):
    cnx = mysql.connector.connect(host=host, user=user, password=password)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')

    # TODO: WRITE CODE HERE
    cursor.execute('USE DMA_team12;')
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS app(
            id VARCHAR(255) NOT NULL,
            title VARCHAR(255) NOT NULL,
            developer_id INT(11) NOT NULL,
            description INT(11) NOT NULL,
            pricing_hint VARCHAR(255),
            PRIMARY KEY(id)
            )Engine = InnoDB DEFAULT CHARSET = utf8mb4;
            ''')
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS app_category(
            app_id VARCHAR(255) NOT NULL,
            category_id VARCHAR(255) NOT NULL,
            PRIMARY KEY(app_id, category_id)
            )Engine = InnoDB DEFAULT CHARSET = utf8mb4;
            ''')
    cursor.execute('''
                CREATE TABLE IF NOT EXISTS category(
                id VARCHAR(255) NOT NULL,
                title VARCHAR(255) NOT NULL,
                PRIMARY KEY(id),
                UNIQUE(title)
                )Engine = InnoDB DEFAULT CHARSET = utf8mb4;
                ''')
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS category_developer(
            category_id VARCHAR(255) NOT NULL,
            developer_id INT(11) NOT NULL,
            PRIMARY KEY(category_id, developer_id)
            )Engine = InnoDB DEFAULT CHARSET = utf8mb4;
            ''')
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS category_user(
            category_id VARCHAR(255) NOT NULL,
            user_id INT(11) NOT NULL,
            PRIMARY KEY(category_id, user_id)
            )Engine = InnoDB DEFAULT CHARSET = utf8mb4;
            ''')
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS developer(
            id INT(11) NOT NULL,
            name VARCHAR(255) NOT NULL, 
            profile_link INT(11) NOT NULL, 
            profile_image TINYINT(1) NOT NULL,
            PRIMARY KEY (id)
            )Engine = InnoDB DEFAULT CHARSET = utf8mb4;
            ''')
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS key_benefit(
            app_id VARCHAR(255) NOT NULL,
            title VARCHAR(255) NOT NULL,
            description INT(11) NOT NULL,
            PRIMARY KEY(app_id, title)
            )Engine = InnoDB DEFAULT CHARSET = utf8mb4;
            ''')
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS message(
            id INT(11) NOT NULL,
            recipient_id INT(11) NOT NULL,
            sent_date DATETIME NOT NULL,
            PRIMARY KEY(id)
            )Engine = InnoDB DEFAULT CHARSET = utf8mb4;
            ''')
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS message_app(
            message_id INT(11) NOT NULL,
            app_id VARCHAR(255) NOT NULL,
            PRIMARY KEY(message_id, app_id)
            )Engine = InnoDB DEFAULT CHARSET = utf8mb4;
            ''')
    cursor.execute('''
               CREATE TABLE IF NOT EXISTS pricing_plan(
               id VARCHAR(255) NOT NULL,
               app_id VARCHAR(255) NOT NULL,
               title VARCHAR(255),
               price VARCHAR(255) NOT NULL,
               PRIMARY KEY(id)
               )Engine = InnoDB DEFAULT CHARSET = utf8mb4;
               ''')
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS reply(
            id INT(11) NOT NULL,
            review_id INT(11) NOT NULL,
            developer_id INT(11) NOT NULL,
            content INT(11) NOT NULL,
            posted_date DATETIME NOT NULL,
            PRIMARY KEY(id),
            UNIQUE(review_id)
            )Engine = InnoDB DEFAULT CHARSET = utf8mb4;
            ''')
    cursor.execute('''
             CREATE TABLE IF NOT EXISTS review(
             id INT(11) NOT NULL,
             app_id VARCHAR(255) NOT NULL,
             user_id INT(11) NOT NULL,
             rating INT(11) NOT NULL,
             body INT(11) NOT NULL,
             helpful_count INT(11) NOT NULL,
             posted_date DATETIME NOT NULL,
             PRIMARY KEY(id)
             )Engine = InnoDB DEFAULT CHARSET = utf8mb4;
             ''')
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS user(
            id INT(11) NOT NULL,
            name VARCHAR(255) NOT NULL,
            PRIMARY KEY(id)
            )Engine = InnoDB DEFAULT CHARSET = utf8mb4;
            ''')
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_follow(
            user_id INT(11) NOT NULL,
            follow_user_id INT(11) NOT NULL,
            follow_date DATETIME NOT NULL,
            PRIMARY KEY(user_id, follow_user_id)
            )Engine = InnoDB DEFAULT CHARSET = utf8mb4;
            ''')

    # TODO: WRITE CODE HERE
    cursor.close()


# Requirement3: insert data
def requirement3(host, user, password, directory):
    cnx = mysql.connector.connect(host=host, user=user, password=password)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')

    # TODO: WRITE CODE HERE
    cursor.execute('USE DMA_team12;')

    f_app = open(directory + '\\app.csv', 'r', encoding='utf-8')
    data_app = f_app.readlines()

    for i in range(1, len(data_app)):

        line_app = data_app[i].replace('\n', '')
        line_app = line_app.split(',')

        if line_app[4]=='':
            sql='INSERT INTO app VALUES (%s,%s,%s,%s,NULL)'
            cursor.execute(sql, (line_app[0], line_app[1], line_app[2], line_app[3]))
        else:
            sql = 'INSERT INTO app VALUES (%s,%s,%s,%s,%s)'
            cursor.execute(sql,(line_app[0], line_app[1], line_app[2], line_app[3], line_app[4]))
    cnx.commit()
    f_app.close()

    f_appCategory = open(directory + '\\app_category.csv', 'r', encoding='utf-8')
    data_appCategory = f_appCategory.readlines()

    for i in range(1, len(data_appCategory)):

        line_appCategory = data_appCategory[i].replace('\n', '')
        line_appCategory = line_appCategory.split(',')
        cursor.execute('''
                        INSERT INTO app_category VALUES (%s,%s)''',
                       (line_appCategory[0], line_appCategory[1]))
    cnx.commit()
    f_appCategory.close()

    f_category = open(directory + '\\category.csv', 'r', encoding='utf-8')
    data_category = f_category.readlines()

    for i in range(1, len(data_category)):

        line_category = data_category[i].replace('\n', '')
        line_category = line_category.split(',')
        cursor.execute('''
                       INSERT INTO category VALUES (%s,%s)''',
                       (line_category[0], line_category[1]))
    cnx.commit()
    f_category.close()

    f_categoryDeveloper = open(directory + '\\category_developer.csv', 'r', encoding='utf-8')
    data_categoryDeveloper = f_categoryDeveloper.readlines()

    for i in range(1, len(data_categoryDeveloper)):

        line_categoryDeveloper = data_categoryDeveloper[i].replace('\n', '')
        line_categoryDeveloper = line_categoryDeveloper.split(',')
        cursor.execute('''
                       INSERT INTO category_developer VALUES (%s,%s)''',
                       (line_categoryDeveloper[0], line_categoryDeveloper[1]))
    cnx.commit()
    f_categoryDeveloper.close()

    f_categoryUser = open(directory + '\\category_user.csv', 'r', encoding='utf-8')
    data_categoryUser = f_categoryUser.readlines()

    for i in range(1, len(data_categoryUser)):

        line_categoryUser = data_categoryUser[i].replace('\n', '')
        line_categoryUser = line_categoryUser.split(',')
        cursor.execute('''
                       INSERT INTO category_user VALUES (%s, %s)''',
                       (line_categoryUser[0], line_categoryUser[1]))
    cnx.commit()
    f_categoryUser.close()

    f_developer = open(directory + '\\developer.csv', 'r', encoding='utf-8')
    data_developer = f_developer.readlines()

    for i in range(1, len(data_developer)):

        line_developer = data_developer[i].replace('\n', '')
        line_developer = line_developer.split(',')
        cursor.execute('''
                        INSERT INTO developer VALUES (%s,%s,%s,%s)''',
                       (line_developer[0], line_developer[1], line_developer[2], line_developer[3]))
    cnx.commit()
    f_developer.close()

    f_keyBenefit = open(directory + '\\key_benefit.csv', 'r', encoding='utf-8')
    data_keyBenefit = f_keyBenefit.readlines()

    for i in range(1, len(data_keyBenefit)):

        line_keyBenefit = data_keyBenefit[i].replace('\n', '')
        line_keyBenefit = line_keyBenefit.split(',')
        cursor.execute('''
                       INSERT INTO key_benefit VALUES (%s,%s,%s);''',
                       (line_keyBenefit[0], line_keyBenefit[1], line_keyBenefit[2]))
    cnx.commit()
    f_keyBenefit.close()

    f_message = open(directory + '\\message.csv', 'r', encoding='utf-8')
    data_message= f_message.readlines()

    for i in range(1, len(data_message)):

        line_message = data_message[i].replace('\n', '')
        line_message = line_message.split(',')
        cursor.execute('''
                        INSERT INTO message VALUES (%s,%s,%s)''',
                       (line_message[0], line_message[1], line_message[2]))
    cnx.commit()
    f_message.close()

    f_messageApp = open(directory + '\\message_app.csv', 'r', encoding='utf-8')
    data_messageApp = f_messageApp.readlines()

    for i in range(1, len(data_messageApp)):

        line_messageApp = data_messageApp[i].replace('\n', '')
        line_messageApp = line_messageApp.split(',')
        cursor.execute('''
                           INSERT INTO message_app VALUES (%s,%s)''',
                       (line_messageApp[0], line_messageApp[1]))
    cnx.commit()
    f_messageApp.close()

    f_pricingPlan = open(directory + '\\pricing_plan.csv', 'r', encoding='utf-8')
    data_pricingPlan = f_pricingPlan.readlines()

    for i in range(1, len(data_pricingPlan)):

        line_pricingPlan = data_pricingPlan[i].replace('\n', '')
        line_pricingPlan = line_pricingPlan.split(',')
        if line_pricingPlan[2]=='':
            sql='INSERT INTO pricing_plan VALUES (%s,%s,NULL,%s)'
            cursor.execute(sql,(line_pricingPlan[0],line_pricingPlan[1],line_pricingPlan[3]))
        else:
            sql = 'INSERT INTO pricing_plan VALUES (%s,%s,%s,%s)'
            cursor.execute(sql,(line_pricingPlan[0], line_pricingPlan[1], line_pricingPlan[2], line_pricingPlan[3]))
    cnx.commit()
    f_pricingPlan.close()

    f_reply=open(directory+'\\reply.csv','r', encoding='utf-8')
    data_reply=f_reply.readlines()

    for i in range(1, len(data_reply)):

        line_reply=data_reply[i].replace('\n','')
        line_reply=line_reply.split(',')
        cursor.execute('''
                    INSERT INTO reply VALUES (%s,%s,%s,%s,%s)''',
                       (line_reply[0],line_reply[1],line_reply[2],line_reply[3],line_reply[4]))
    cnx.commit()
    f_reply.close()

    f_review = open(directory + '\\review.csv', 'r', encoding='utf-8')
    data_review = f_review.readlines()

    for i in range(1, len(data_review)):

        line_review = data_review[i].replace('\n', '')
        line_review = line_review.split(',')
        cursor.execute('''
                           INSERT INTO review VALUES (%s,%s,%s,%s,%s,%s,%s)''',
                       (line_review[0], line_review[1], line_review[2], line_review[3],line_review[4],line_review[5],line_review[6]))
    cnx.commit()
    f_review.close()

    f_user = open(directory + '\\user.csv', 'r', encoding='utf-8')
    data_user = f_user.readlines()

    for i in range(1, len(data_user)):

        line_user = data_user[i].replace('\n', '')
        line_user = line_user.split(',')
        cursor.execute('''
                           INSERT INTO user VALUES (%s,%s)''',
                       (line_user[0], line_user[1]))
    cnx.commit()
    f_user.close()

    f_userFollow = open(directory + '\\user_follow.csv', 'r', encoding='utf-8')
    data_userFollow = f_userFollow.readlines()

    for i in range(1, len(data_userFollow)):

        line_userFollow = data_userFollow[i].replace('\n', '')
        line_userFollow = line_userFollow.split(',')
        cursor.execute('''
                           INSERT INTO user_follow VALUES (%s,%s,%s)''',
                       (line_userFollow[0], line_userFollow[1], line_userFollow[2]))
    cnx.commit()
    f_userFollow.close()


    # TODO: WRITE CODE HERE
    cursor.close()


# Requirement4: add constraint (foreign key)
def requirement4(host, user, password):
    cnx = mysql.connector.connect(host=host, user=user, password=password)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')

    # TODO: WRITE CODE HERE
    cursor.execute('USE DMA_team12')
    cursor.execute('ALTER TABLE app ADD CONSTRAINT FOREIGN KEY (developer_id) REFERENCES developer(id);')
    cursor.execute('ALTER TABLE reply ADD CONSTRAINT FOREIGN KEY (review_id) REFERENCES review(id);')
    cursor.execute('ALTER TABLE reply ADD CONSTRAINT FOREIGN KEY (developer_id) REFERENCES developer(id);')
    cursor.execute('ALTER TABLE review ADD CONSTRAINT FOREIGN KEY (app_id) REFERENCES app(id);')
    cursor.execute('ALTER TABLE message ADD CONSTRAINT FOREIGN KEY (recipient_id) REFERENCES user(id);')
    cursor.execute('ALTER TABLE pricing_plan ADD CONSTRAINT FOREIGN KEY (app_id) REFERENCES app(id);')
    cursor.execute('ALTER TABLE key_benefit ADD CONSTRAINT FOREIGN KEY (app_id) REFERENCES app(id);')
    cursor.execute('ALTER TABLE app_category ADD CONSTRAINT FOREIGN KEY (app_id) REFERENCES app(id);')
    cursor.execute('ALTER TABLE app_category ADD CONSTRAINT FOREIGN KEY (category_id) REFERENCES category(id);')
    cursor.execute('ALTER TABLE category_user ADD CONSTRAINT FOREIGN KEY (category_id) REFERENCES category(id);')
    cursor.execute('ALTER TABLE category_user ADD CONSTRAINT FOREIGN KEY (user_id) REFERENCES user(id);')
    cursor.execute('ALTER TABLE category_developer ADD CONSTRAINT FOREIGN KEY (category_id) REFERENCES category(id);')
    cursor.execute('ALTER TABLE category_developer ADD CONSTRAINT FOREIGN KEY (developer_id) REFERENCES developer(id);')
    cursor.execute('ALTER TABLE message_app ADD CONSTRAINT FOREIGN KEY (message_id) REFERENCES message(id);')
    cursor.execute('ALTER TABLE message_app ADD CONSTRAINT FOREIGN KEY (app_id) REFERENCES app(id);')
    cursor.execute('ALTER TABLE user_follow ADD CONSTRAINT FOREIGN KEY (user_id) REFERENCES user(id);')
    cursor.execute('ALTER TABLE user_follow ADD CONSTRAINT FOREIGN KEY (follow_user_id) REFERENCES user(id);')
    # TODO: WRITE CODE HERE
    cnx.commit()
    cursor.close()


# TODO: REPLACE THE VALUES OF FOLLOWING VARIABLES
host = '127.0.0.1'
user = 'root'
password = 'vision8690@'
directory_in ='C:/Users/User/Downloads/DMA_project1/DMA_project1/dataset'

requirement1(host=host, user=user, password=password)
requirement2(host=host, user=user, password=password)
requirement3(host=host, user=user, password=password, directory=directory_in)
requirement4(host=host, user=user, password=password)

