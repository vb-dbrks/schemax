import React from 'react';
import ComponentCreator from '@docusaurus/ComponentCreator';

export default [
  {
    path: '/schematic/',
    component: ComponentCreator('/schematic/', '7dc'),
    routes: [
      {
        path: '/schematic/',
        component: ComponentCreator('/schematic/', '863'),
        routes: [
          {
            path: '/schematic/',
            component: ComponentCreator('/schematic/', 'a00'),
            routes: [
              {
                path: '/schematic/guide/architecture/',
                component: ComponentCreator('/schematic/guide/architecture/', 'bb1'),
                exact: true,
                sidebar: "tutorialSidebar"
              },
              {
                path: '/schematic/guide/development/',
                component: ComponentCreator('/schematic/guide/development/', '4fd'),
                exact: true,
                sidebar: "tutorialSidebar"
              },
              {
                path: '/schematic/guide/quickstart/',
                component: ComponentCreator('/schematic/guide/quickstart/', 'f1b'),
                exact: true,
                sidebar: "tutorialSidebar"
              },
              {
                path: '/schematic/reference/cli/',
                component: ComponentCreator('/schematic/reference/cli/', '84e'),
                exact: true,
                sidebar: "tutorialSidebar"
              },
              {
                path: '/schematic/reference/testing/',
                component: ComponentCreator('/schematic/reference/testing/', 'dfe'),
                exact: true,
                sidebar: "tutorialSidebar"
              },
              {
                path: '/schematic/reference/workflows/',
                component: ComponentCreator('/schematic/reference/workflows/', '4c1'),
                exact: true,
                sidebar: "tutorialSidebar"
              },
              {
                path: '/schematic/',
                component: ComponentCreator('/schematic/', '739'),
                exact: true,
                sidebar: "tutorialSidebar"
              }
            ]
          }
        ]
      }
    ]
  },
  {
    path: '*',
    component: ComponentCreator('*'),
  },
];
